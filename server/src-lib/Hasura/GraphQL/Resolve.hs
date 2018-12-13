{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Hasura.GraphQL.Resolve
  ( resolveSelSet
  ) where

import           Hasura.Prelude

import qualified Data.Aeson                             as J
import qualified Data.ByteString.Base64                 as B64
import qualified Data.ByteString.Lazy                   as BL
import qualified Data.HashMap.Strict                    as Map
import qualified Data.Text                              as T
import           Data.Text.Encoding                     (encodeUtf8)
import qualified Database.PG.Query                      as Q
import qualified Language.GraphQL.Draft.Syntax          as G

import           Hasura.GraphQL.Schema
import           Hasura.GraphQL.Transport.HTTP.Protocol
import           Hasura.GraphQL.Validate.Context        (getTyInfo)
import           Hasura.GraphQL.Validate.Field
import           Hasura.GraphQL.Validate.Types
import           Hasura.RQL.Types
import           Hasura.SQL.Types
import           Hasura.SQL.Value

import           Hasura.GraphQL.Resolve.Context
import           Hasura.GraphQL.Resolve.InputValue      (asGQTyID)
import qualified Hasura.GraphQL.Resolve.Insert          as RI
import           Hasura.GraphQL.Resolve.Introspect
import qualified Hasura.GraphQL.Resolve.Mutation        as RM
import qualified Hasura.GraphQL.Resolve.Select          as RS


-- {-# SCC buildTx #-}
buildTx :: UserInfo -> GCtx -> Field -> Q.TxE QErr BL.ByteString
buildTx userInfo gCtx fld = do
  opCxt <- getOpCtx $ _fName fld
  join $ fmap fst $ runConvert (fldMap, orderByCtx, insCtxMap, tyMap) $ case opCxt of

    OCSelect tn permFilter permLimit hdrs ->
      validateHdrs hdrs >> RS.convertSelect tn permFilter permLimit fld
    OCSelectConn tn permFilter permLimit hdrs ->
      validateHdrs hdrs >> RS.convertSelectConn tn permFilter permLimit fld
    OCSelectNode tblFldMap -> do
      withPathK "selectionSet" $ withArg (_fArguments fld) "id" $
        \idArg -> asSelectByPK tblFldMap idArg
    OCSelectPkey tn permFilter hdrs ->
      validateHdrs hdrs >> RS.convertSelectByPKey tn permFilter fld
      -- RS.convertSelect tn permFilter fld
    OCSelectAgg tn permFilter permLimit hdrs ->
      validateHdrs hdrs >> RS.convertAggSelect tn permFilter permLimit fld
    OCInsert tn hdrs    ->
      validateHdrs hdrs >> RI.convertInsert roleName tn fld
      -- RM.convertInsert (tn, vn) cols fld
    OCUpdate tn permFilter hdrs ->
      validateHdrs hdrs >> RM.convertUpdate tn permFilter fld
      -- RM.convertUpdate tn permFilter fld
    OCDelete tn permFilter hdrs ->
      validateHdrs hdrs >> RM.convertDelete tn permFilter fld
      -- RM.convertDelete tn permFilter fld
  where
    roleName = userRole userInfo
    opCtxMap = _gOpCtxMap gCtx
    fldMap = _gFields gCtx
    tyMap = _gTypes gCtx
    orderByCtx = _gOrdByCtx gCtx
    insCtxMap = _gInsCtxMap gCtx

    getOpCtx f =
      onNothing (Map.lookup f opCtxMap) $ throw500 $
      "lookup failed: opctx: " <> showName f

    validateHdrs hdrs = do
      let receivedVars = userVars userInfo
      forM_ hdrs $ \hdr ->
        unless (isJust $ getVarVal hdr receivedVars) $
        throw400 NotFound $ hdr <<> " header is expected but not found"

    asSelectByPK tblFldMap arg = do
      CurRowId tn valMap <- parseRowId arg
      pkTblFldInfo <- validateRowIdTbl tn tblFldMap
      (_,permFilter,hdrs) <- asSelPKCtx $ _fiName pkTblFldInfo
      pkArgs <- fmap Map.fromList $ mapM (validateRowIdPKArgs $ _fiParams pkTblFldInfo) $ Map.toList valMap
      let pkTblFldTy = getBaseTy $ _fiTy pkTblFldInfo
      -- Merging fields for conditional selection sets are deferred until types are resolved (so as to avoid unnecessary checks at validate)
      pkSelSet <- mergeFields $ fromMaybe (_fSelSet fld) $ Map.lookup pkTblFldTy $ _fCondSelSet fld
      validateHdrs hdrs
      RS.convertSelectByPKey tn permFilter $ Field
        { _fAlias      = _fAlias fld
        , _fArguments  = pkArgs
        , _fName       = _fiName pkTblFldInfo
        , _fType       = pkTblFldTy
        , _fSelSet     = pkSelSet
        , _fCondSelSet = Map.empty
        }

    asSelPKCtx fn = case (Map.lookup fn opCtxMap) of
      Just (OCSelectPkey t f h) -> return (t,f,h)
      _ -> parseIDErr $ "Invalid context conversion for node field as non-primary key field" <> showName fn

    parseRowId a = do
      idTxt <- asGQTyID a
      either decodeIDVE return $ (B64.decode >=> J.eitherDecodeStrict) $ encodeUtf8 $ T.replace "\n" "" idTxt

    decodeIDVE e = throwVE $ "Error while decoding ID: " <> T.pack e
    parseIDVE e = throwVE $ "Error while parsing ID: " <> e
    parseIDErr e = throw500 $ "Error while parsing ID: " <> e

    validateRowIdTbl t tFldMap = do
      fn <- onNothing (Map.lookup t tFldMap) $ parseIDVE $
        "Invalid table " <> qualTableToTxt t
      onNothing (Map.lookup fn $ _otiFields $ _gQueryRoot gCtx) $
        parseIDErr $ "Field " <> G.unName fn <> " is not present in query root"

    validateRowIdPKArgs paramsMap (k,val) = do
      tn <- getNamedArgTy
      (,) argName <$> validateIDPGColVal tn val
      where
        argName = G.Name $ getPGColTxt k
        getNamedArgTy = do
          argTy <- getArgTy
          case argTy of
            G.TypeNamed _ nt -> return nt
            G.TypeList _ _ -> throwVE $ "Error parsing ID: Unexpected list type for argument " <> showName argName
        getArgTy =
          onNothing (_iviType <$> Map.lookup argName paramsMap ) $ throwVE $
          "Error parsing ID: No such argument " <> showName argName <> " is expected"
    validateIDPGColVal nt val = do
      tyInfo <- getTyInfo nt
      case tyInfo of
        TIScalar (ScalarTyInfo _ (Right pgColTy) _) ->
          fmap (AGScalar pgColTy . Just) $ runAesonParser (parsePGValue pgColTy) val
        _ -> throwVE $ "Error parsing ID: Unexpected non-scalar type for column " <> showNamedTy nt

-- {-# SCC resolveFld #-}
resolveFld
  :: UserInfo -> GCtx
  -> G.OperationType
  -> Field
  -> Q.TxE QErr BL.ByteString
resolveFld userInfo gCtx opTy fld =
  case _fName fld of
    "__type"     -> J.encode <$> runReaderT (typeR fld) gCtx
    "__schema"   -> J.encode <$> runReaderT (schemaR fld) gCtx
    "__typename" -> return $ J.encode $ mkRootTypeName opTy
    _            -> buildTx userInfo gCtx fld
  where
    mkRootTypeName :: G.OperationType -> Text
    mkRootTypeName = \case
      G.OperationTypeQuery        -> "query_root"
      G.OperationTypeMutation     -> "mutation_root"
      G.OperationTypeSubscription -> "subscription_root"

resolveSelSet
  :: UserInfo -> GCtx
  -> G.OperationType
  -> SelSet
  -> Q.TxE QErr BL.ByteString
resolveSelSet userInfo gCtx opTy fields =
  fmap mkJSONObj $ forM (toList fields) $ \fld -> do
    fldResp <- resolveFld userInfo gCtx opTy fld
    return (G.unName $ G.unAlias $ _fAlias fld, fldResp)
