import yaml
from validate import check_query_f, check_query
from super_classes import DefaultTestSelectQueries

class TestGraphqlIntrospection(DefaultTestSelectQueries):

    def test_introspection(self, hge_ctx):
        with open(self.dir() + "/introspection.yaml") as c:
            conf = yaml.safe_load(c)
        code, resp = check_query(hge_ctx, conf)
        assert code == 200, resp
        hasArticle = False
        hasArticleAuthorFKRel = False
        hasArticleAuthorManualRel = False
        queriesPresent = {
            'author_by_pk'  : False,
            'article_by_pk' : False,
            'user_by_pk'    : False
        }
        for t in resp['data']['__schema']['types']:
            if t['name'] == 'article':
                hasArticle = True
                for fld in t['fields']:
                    if fld['name'] == 'author_obj_rel_manual':
                        hasArticleAuthorManualRel = True
                        assert fld['type']['kind'] == 'OBJECT'
                    elif fld['name'] == 'author_obj_rel_fk':
                        hasArticleAuthorFKRel = True
                        assert fld['type']['kind'] == 'NON_NULL'
            elif t['name'] == 'query_root':
                for fld in t['fields']:
                    if fld['name'] in queriesPresent:
                        queriesPresent[fld['name']] = True
        assert hasArticle , "type article not present"
        assert hasArticleAuthorFKRel, "Relationship 'author_obj_rel_fk' for type article is not present"
        assert hasArticleAuthorManualRel, "Relationship 'author_obj_rel_manual' for type article is not present"
        for query in ['author_by_pk','article_by_pk']:
            assert queriesPresent[query] == True, "Query '"+ query + "' is not present"
        for query in ['user_by_pk']:
            assert queriesPresent[query] == False, "Query '"+ query + "' is present, which it should not be as the table does not have primary key"
        self.assertMutations(resp, self.allPossibleMutations())


    def test_introspection_user(self, hge_ctx):
        with open(self.dir() + "/introspection_user_role.yaml") as c:
            conf = yaml.safe_load(c)
        code, resp = check_query(hge_ctx, conf)
        assert code == 200, resp

        expPresentMutations = set(['insert_article','update_author','delete_user'])
        self.assertMutations(resp, expPresentMutations )

    def assertMutations(self, resp, expPresentMutations):
        expAbsentMutations = self.allPossibleMutations() - expPresentMutations
        mutationIsPresent =  {k: False for k in self.allPossibleMutations()}
        for t in resp['data']['__schema']['types']:
            if t['name'] == 'mutation_root':
                for oper in t['fields']:
                    name = oper['name']
                    if name in mutationIsPresent:
                        mutationIsPresent[name] = True
        errors = []
        for m in expPresentMutations:
            if not mutationIsPresent[m]:
                errors.append("Error: mutation " + m + " is not present for user role")
        for m in expAbsentMutations:
            if mutationIsPresent[m]:
                errors.append("Error: mutation " + m + " is present for user role, which it should not be")
        assert len(errors) == 0, errors

    @classmethod
    def allPossibleMutations(cls):
        return set( [o + '_' + t
            for o in ['insert','update','delete']
            for t in ['author','article','user']
        ] )

    @classmethod
    def dir(cls):
        return "queries/graphql_introspection"
