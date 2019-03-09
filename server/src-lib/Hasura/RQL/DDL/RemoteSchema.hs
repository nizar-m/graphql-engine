module Hasura.RQL.DDL.RemoteSchema
  ( runAddRemoteSchema
  , runReloadRemoteSchema
  , runRemoveRemoteSchema
  , writeRemoteSchemasToCache
  , refreshGCtxMapInSchema
  , fetchRemoteSchemas
  , addRemoteSchemaP2
  ) where

import           Hasura.Prelude

import qualified Data.Aeson                  as J
import qualified Data.ByteString.Lazy        as BL
import qualified Data.HashMap.Strict         as Map
import qualified Database.PG.Query           as Q

import           Hasura.GraphQL.RemoteServer
import           Hasura.RQL.Types

import qualified Hasura.GraphQL.Context      as GC
import qualified Hasura.GraphQL.Schema       as GS

runAddRemoteSchema
  :: ( QErrM m, UserInfoM m, CacheRWM m, MonadTx m
     , MonadIO m
     , HasHttpManager m
     )
  => AddRemoteSchemaQuery -> m RespBody
runAddRemoteSchema q = do
  adminOnly
  addRemoteSchemaP2 q

addRemoteSchemaP2
  :: ( QErrM m
     , CacheRWM m
     , MonadTx m
     , MonadIO m
     , HasHttpManager m
     )
  => AddRemoteSchemaQuery
  -> m BL.ByteString
addRemoteSchemaP2 q@(AddRemoteSchemaQuery name def _) = do
  rsi <- validateRemoteSchemaDef def
  manager <- askHttpManager
  sc <- askSchemaCache
  let defRemoteGCtx = scDefaultRemoteGCtx sc
  remoteGCtx <- fetchRemoteSchema manager name rsi
  newDefGCtx <- mergeGCtx defRemoteGCtx $ convRemoteGCtx remoteGCtx
  newHsraGCtxMap <- GS.mkGCtxMap (scTables sc) (scFunctions sc)
  newGCtxMap <- mergeRemoteSchema newHsraGCtxMap newDefGCtx
  liftTx $ addRemoteSchemaToCatalog q
  addRemoteSchemaToCache newGCtxMap newDefGCtx name rsi remoteGCtx
  return successMsg

addRemoteSchemaToCache
  :: CacheRWM m
  => GS.GCtxMap
  -> GS.GCtx
  -> RemoteSchemaName
  -> RemoteSchemaInfo
  -> GC.RemoteGCtx
  -> m ()
addRemoteSchemaToCache gCtxMap defGCtx name rmDef rmCtx = do
  sc <- askSchemaCache
  let resolvers = scRemoteResolvers sc
  let remGCtxs  = scRemoteGCtxs sc
  writeSchemaCache sc { scRemoteResolvers = Map.insert name rmDef resolvers
                      , scRemoteGCtxs = Map.insert name rmCtx remGCtxs
                      , scGCtxMap = gCtxMap
                      , scDefaultRemoteGCtx = defGCtx
                      }

writeRemoteSchemasToCache
  :: CacheRWM m
  => GS.GCtxMap -> RemoteSchemaMap -> RemoteGCtxMap -> m ()
writeRemoteSchemasToCache gCtxMap resolvers rGCtxs = do
  sc <- askSchemaCache
  writeSchemaCache sc { scRemoteResolvers = resolvers
                      , scGCtxMap = gCtxMap
                      , scRemoteGCtxs = rGCtxs
                      }

refreshGCtxMapInSchema
  :: (CacheRWM m, MonadError QErr m) => m ()
refreshGCtxMapInSchema = do
  sc <- askSchemaCache
  gCtxMap <- GS.mkGCtxMap (scTables sc) (scFunctions sc)
  (mergedGCtxMap, defGCtx) <- mergeSchemas (scRemoteGCtxs sc) gCtxMap
  writeSchemaCache sc { scGCtxMap = mergedGCtxMap
                      , scDefaultRemoteGCtx = defGCtx }

reloadRemoteSchemaP1
  :: (UserInfoM m, QErrM m)
  => ReloadRemoteSchemaQuery -> m ReloadRemoteSchemaQuery
reloadRemoteSchemaP1 q = adminOnly >> return q

reloadRemoteSchemaP2
  :: ( MonadError QErr m
     , HasHttpManager m
     , MonadIO m
     , CacheRWM m
     )
  => RemoveRemoteSchemaQuery -> m BL.ByteString
reloadRemoteSchemaP2 q = do
   sc <- askSchemaCache
   let sn = _rrsqName q
   rsi <- onNothing (Map.lookup sn $ scRemoteResolvers sc) $ throw404 $ "Could not find remote schema with name " <> sn
   httpMgr <- askHttpManager
   rs <- fetchRemoteSchema httpMgr sn rsi
   let schemaHasChanged = Just rs /= Map.lookup sn (scRemoteGCtxs sc)
   when schemaHasChanged $ do
     let rCtxMap' = Map.insert sn rs $ scRemoteGCtxs sc
     writeSchemaCache $ sc { scRemoteGCtxs = rCtxMap' }
     refreshGCtxMapInSchema
   return successMsg

runReloadRemoteSchema
  :: ( UserInfoM m
     , MonadError QErr m
     , HasHttpManager m
     , MonadIO m
     , CacheRWM m
     )
  => ReloadRemoteSchemaQuery -> m BL.ByteString
runReloadRemoteSchema q = reloadRemoteSchemaP1 q >>= reloadRemoteSchemaP2

runRemoveRemoteSchema
  :: ( QErrM m
     , UserInfoM m
     , CacheRWM m
     , MonadTx m
     )
  => RemoveRemoteSchemaQuery -> m RespBody
runRemoveRemoteSchema q =
  removeRemoteSchemaP1 q >>= removeRemoteSchemaP2

removeRemoteSchemaP1
  :: (UserInfoM m, QErrM m)
  => RemoveRemoteSchemaQuery -> m RemoveRemoteSchemaQuery
removeRemoteSchemaP1 q = adminOnly >> return q

removeRemoteSchemaP2
  :: ( QErrM m
     , CacheRWM m
     , MonadTx m
     )
  => RemoveRemoteSchemaQuery -> m BL.ByteString
removeRemoteSchemaP2 (RemoveRemoteSchemaQuery name) = do
  mSchema <- liftTx $ fetchRemoteSchemaDef name
  _ <- liftMaybe (err400 NotExists "no such remote schema") mSchema
  --url <- either return getUrlFromEnv eUrlVal

  sc <- askSchemaCache
  let newResolvers = Map.delete name $ scRemoteResolvers sc
      newRGCtxs = Map.delete name $ scRemoteGCtxs sc
  -- Write the modified remote resolvers and Gctxs to cache
  writeSchemaCache $ sc
    { scRemoteResolvers = newResolvers
    , scRemoteGCtxs = newRGCtxs
    }
  -- Refresh combined GCtx map
  refreshGCtxMapInSchema
  liftTx $ removeRemoteSchemaFromCatalog name
  return successMsg


addRemoteSchemaToCatalog
  :: AddRemoteSchemaQuery
  -> Q.TxE QErr ()
addRemoteSchemaToCatalog (AddRemoteSchemaQuery name def comment) =
  Q.unitQE defaultTxErrorHandler [Q.sql|
    INSERT into hdb_catalog.remote_schemas
      (name, definition, comment)
      VALUES ($1, $2, $3)
  |] (name, Q.AltJ $ J.toJSON def, comment) True


removeRemoteSchemaFromCatalog :: Text -> Q.TxE QErr ()
removeRemoteSchemaFromCatalog name =
  Q.unitQE defaultTxErrorHandler [Q.sql|
    DELETE FROM hdb_catalog.remote_schemas
      WHERE name = $1
  |] (Identity name) True


fetchRemoteSchemaDef :: Text -> Q.TxE QErr (Maybe RemoteSchemaDef)
fetchRemoteSchemaDef name =
  fmap (fromRow . runIdentity) <$> Q.withQE defaultTxErrorHandler
    [Q.sql|
     SELECT definition from hdb_catalog.remote_schemas
       WHERE name = $1
     |] (Identity name) True
  where
    fromRow (Q.AltJ def) = def

fetchRemoteSchemas :: Q.TxE QErr [AddRemoteSchemaQuery]
fetchRemoteSchemas =
  map fromRow <$> Q.listQE defaultTxErrorHandler
    [Q.sql|
     SELECT name, definition, comment
       FROM hdb_catalog.remote_schemas
     |] () True
  where
    fromRow (n, Q.AltJ def, comm) = AddRemoteSchemaQuery n def comm
