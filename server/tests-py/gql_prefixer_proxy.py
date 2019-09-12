from http import HTTPStatus
import requests
import json
import re
import graphql

from webserver import RequestHandler, WebServer, MkHandlers, Response

def first(iterable, key, default=None):
   return next( (x for x in iterable if key(x)), default)

def get_introspect_types(introspect):
   return json_get(introspect, ['data', '__schema', 'types'])

def has_name(name):
   return lambda x : x['name'] == name

def get_fld_by_name(flds, name):
   return first(flds, has_name(name))

def get_ty_by_name(types, name):
   return first(types, has_name(name))

def json_get(obj, path, default=None):
   if obj == None:
      return None
   elif len(path) == 0:
      return obj
   elif len(path) == 1:
      return obj.get(path[0], default)
   else:
      return json_get(obj.get(path[0],{ }), path[1:])

def get_base_ty(fld):
   base_ty = fld['type']
   while not base_ty['name']:
      base_ty = base_ty['ofType']
   return base_ty

class GraphQLPrefixerProxy(RequestHandler):
   """
   This proxy adds a prefix to all the object type names (except for the default ones), 
    and also to the top level fields of queries, mutations and subscriptions.

   Further to enable adding this as remote schema to the server it is proxying,
   It also deletes all the types starting with the prefix, and the top-level nodes starting with same prefix.
   """

   def __init__(self, gql_url, headers, prefix):
      self.gql_url = gql_url
      self.headers = headers
      self.prefix = prefix

   def _is_non_def_obj_ty(self, ty):
      return ty['kind'] == 'OBJECT' and not ty['name'].startswith('__')

   def _add_name_prefix(self, obj):
      assert not obj['name'].startswith(self.prefix), obj
      obj['name'] = self.prefix + obj['name']

   def get(self, request):
      return Response(HTTPStatus.METHOD_NOT_ALLOWED)

   def _assert_prefixes(self, introspect):
      types = get_introspect_types(introspect)
      if not types:
         return

      for ty in types:
         if self._is_non_def_obj_ty(ty):
            assert ty['name'].startswith(self.prefix), ty
            for fld in ty['fields']:
               base_ty = get_base_ty(fld)
               if self._is_non_def_obj_ty(base_ty):
                  assert base_ty['name'].startswith(self.prefix), fld
               assert not base_ty['name'].startswith(self.prefix*2), fld
         assert not ty['name'].startswith(self.prefix*2), ty

      for oper_type in ['queryType', 'mutationType', 'subscriptionType']:
         ty_name = json_get(introspect, ['data', '__schema', oper_type, 'name'])
         if not ty_name:
            continue
         assert ty_name.startswith(self.prefix), ty_name
         assert not ty_name.startswith(2*self.prefix), ty_name
         ty = get_ty_by_name(types, ty_name)
         if not ty:
            continue

         for fld in ty['fields']:
            assert fld['name'].startswith(self.prefix), fld
            assert not fld['name'].startswith(2*self.prefix), fld



   def _mod_types_introspect(self, introspect):
      types = get_introspect_types(introspect)
      if not types:
         return
      # Add prefix to all non-default types
      # If types start with the given prefix, remove them
      # This would avoid cycles being created when this proxy is added as remote graphql to the original graphql server
      to_remove_types=[]
      for ty in types:
         if not self._is_non_def_obj_ty(ty):
            continue
         if ty['name'].startswith(self.prefix):
            to_remove_types.append(ty)
         else:
            self._add_name_prefix(ty)
            # Add prefix to the types of fields as well
            to_remove_flds = []
            for fld in ty['fields']:
               base_ty = get_base_ty(fld)
               if base_ty['name'].startswith(self.prefix):
                  to_remove_flds.append(fld)
               elif self._is_non_def_obj_ty(base_ty):
                  self._add_name_prefix(base_ty)
            for fld in to_remove_flds:
               ty['fields'].remove(fld)

      for ty in to_remove_types:
         types.remove(ty)

      # Add prefix to the operation types
      for oper_type in ['queryType', 'mutationType', 'subscriptionType']:
         ty_info = json_get(introspect, ['data', '__schema', oper_type])
         if ty_info and not ty_info['name'].startswith(self.prefix):
            self._add_name_prefix(ty_info)

   # With queries we need to strip prefix from top level fields (if present)
   def _query_mod_top_level_fields(self, req):
      errors = []
      query = graphql.parse(req['query'], no_location=True)
      for oper in query.definitions:
         if not getattr(oper, 'operation', None):
            continue
         for top_fld in oper.selection_set.selections:
            if top_fld.name.value.startswith(self.prefix):
               if not top_fld.alias:
                  top_fld.alias = graphql.NameNode(value=top_fld.name.value)
               # Remove prefix
               top_fld.name.value = re.sub('^'+ re.escape(self.prefix), '', top_fld.name.value)
            elif top_fld.name.value not in ['__schema', '__type', '__typename' ]:
               errors.append('Unknown field ' + top_fld.name.value)
      req['query'] = graphql.print_ast(query)
      return errors


   # Add prefix for top level fields of all the operation types
   def _mod_top_level_fields_introspect(self, introspect):
      types = get_introspect_types(introspect)
      if not types:
         return

      for oper_type in ['queryType', 'mutationType', 'subscriptionType']:
         ty_name = json_get(introspect, ['data', '__schema', oper_type, 'name'])
         if not ty_name:
            continue
         ty = get_ty_by_name(types, ty_name)
         if not ty:
            continue
         to_drop_fields = []
         for fld in ty['fields']:
            if fld['name'].startswith(self.prefix):
               to_drop_fields.append(fld)
            else:
               self._add_name_prefix(fld)
         for fld in to_drop_fields:
            ty['fields'].remove(fld)

   def post(self, request):
      input_query = request.json.copy()
      if not request.json:
         return Response(HTTPStatus.BAD_REQUEST)
      errors = self._query_mod_top_level_fields(request.json)
      if errors:
         print('ERROR:',errors)
         json_out = {'errors': errors}
      else:
         if request.json.get('query') != input_query.get('query'):
            print ("input query:", input_query)
            print ("proxied query:", request.json)
         print("Prefixer proxy: GrahpQL url:", self.gql_url)
         resp = requests.post(self.gql_url, json.dumps(request.json), headers=self.headers)
         json_out = resp.json()
         if json_out.get('errors'):
            print('ERROR:', json_out['errors'])
         self._mod_top_level_fields_introspect(json_out)
         self._mod_types_introspect(json_out)
         self._assert_prefixes(json_out)
      return Response(HTTPStatus.OK, json_out, {'Content-Type': 'application/json'})

handlers = MkHandlers({ '/graphql': GraphQLPrefixerProxy })

def MkGQLPrefixerProxy(gql_url, headers={}, prefix='prefixer_proxy_'):
   class _GQLPrefixerProxy(GraphQLPrefixerProxy):
      def __init__(self):
         super().__init__(gql_url, headers, prefix)
   return _GQLPrefixerProxy

def create_server(gql_url, headers={}, host='127.0.0.1', port=5000):
   return WebServer((host, port), MkGQLPrefixerProxy(gql_url, headers) )
