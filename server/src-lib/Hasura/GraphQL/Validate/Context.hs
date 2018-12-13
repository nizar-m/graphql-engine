{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude     #-}
{-# LANGUAGE OverloadedStrings     #-}

module Hasura.GraphQL.Validate.Context
  ( ValidationCtx(..)
  , getIFaceFieldInfo
  , getFieldInfo
  , getInpFieldInfo
  , getTyInfo
  , getSSOTyInfo
  , getTyInfoVE
  , module Hasura.GraphQL.Utils
  ) where

import           Hasura.Prelude

import qualified Data.HashMap.Strict           as Map
import qualified Language.GraphQL.Draft.Syntax as G

import           Data.Has
import           Hasura.GraphQL.Utils
import           Hasura.GraphQL.Validate.Types
import           Hasura.RQL.Types

getFieldInfo
  :: ( MonadError QErr m)
  => ObjTyInfo -> G.Name -> m ObjFldInfo
getFieldInfo oti fldName =
  onNothing (Map.lookup fldName $ _otiFields oti) $ throwVE $
  "field " <> showName fldName <>
  " not found in type: " <> showNamedTy (_otiName oti)

getIFaceFieldInfo
  :: ( MonadError QErr m)
  => IFaceTyInfo -> G.Name -> m ObjFldInfo
getIFaceFieldInfo ifti fldName =
  onNothing (Map.lookup fldName $ _ifFields ifti) $ throwVE $
  "field " <> showName fldName <>
  " not found in type: " <> showNamedTy (_ifName ifti)

getInpFieldInfo
  :: ( MonadError QErr m)
  => InpObjTyInfo -> G.Name -> m G.GType
getInpFieldInfo tyInfo fldName =
  fmap _iviType $ onNothing (Map.lookup fldName $ _iotiFields tyInfo) $
  throwVE $ "field " <> showName fldName <>
  " not found in type: " <> showNamedTy (_iotiName tyInfo)

data ValidationCtx
  = ValidationCtx
  { _vcTypeMap    :: !TypeMap
  -- these are in the scope of the operation
  , _vcVarVals    :: !AnnVarVals
  -- all the fragments
  , _vcFragDefMap :: !FragDefMap
  } deriving (Show, Eq)

instance Has TypeMap ValidationCtx where
  getter = _vcTypeMap
  modifier f ctx = ctx { _vcTypeMap = f $ _vcTypeMap ctx }

getTyInfo
  :: ( MonadReader r m , Has TypeMap r
     , MonadError QErr m)
  => G.NamedType
  -> m TypeInfo
getTyInfo namedTy = do
  tyMap <- asks getter
  onNothing (Map.lookup namedTy tyMap) $
    throw500 $ "type info not found for: " <> showNamedTy namedTy

getSSOTyInfo
  :: ( MonadReader r m, Has TypeMap r
     , MonadError QErr m)
  => G.NamedType
  -> m SelSetObj
getSSOTyInfo namedTy = do
  tyInfo <- getTyInfo namedTy
  onNothing (getSSOTyM tyInfo) $ throw500 $ "Fragments cannot be applied on type: " <> showNamedTy namedTy
  

getTyInfoVE
  :: ( MonadReader r m , Has TypeMap r
     , MonadError QErr m)
  => G.NamedType
  -> m TypeInfo
getTyInfoVE namedTy = do
  tyMap <- asks getter
  onNothing (Map.lookup namedTy tyMap) $
    throwVE $ "no such type exists in the schema: " <> showNamedTy namedTy
