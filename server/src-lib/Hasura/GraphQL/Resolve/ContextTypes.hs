{-# LANGUAGE TemplateHaskell #-}
module Hasura.GraphQL.Resolve.ContextTypes where

import           Hasura.Prelude

import           Control.Lens.TH
import qualified Data.Aeson                        as J
import qualified Data.HashMap.Strict               as Map
import qualified Data.Vector                       as V
import qualified Language.GraphQL.Draft.Syntax     as G

import           Hasura.RQL.Types.BoolExp
import           Hasura.RQL.Types.Common
import           Hasura.RQL.Types.SchemaCacheTypes
import           Hasura.SQL.Types

data CurRowId
  = CurRowId
  { criTbl   :: QualifiedTable
  , criPKVal :: Map.HashMap PGCol J.Value
  }
  deriving (Show, Eq)

instance J.FromJSON CurRowId  where
  parseJSON = J.withArray "CurRowId" $ \v -> do
    when (V.length v /= 3) $ fail "expecting an array of 3 objects"
    let [s,n,c] = V.toList v
    t <- QualifiedTable <$> J.withText "String" (return . SchemaName) s
        <*> J.withText "String" (return . TableName) n
    CurRowId t <$> J.parseJSON c


data TyFldInfo
  = TFICol
    { _tficColInfo :: PGColInfo }
  | TFIRel
    { _tfirRelInfo :: RelInfo
    , _tfirIsAgg   :: Bool
    , _tfirBoolExp :: AnnBoolExpSQL
    , _tfirLimit   :: (Maybe Int)
    }
  | TFIRowId
    { _tfirTable :: QualifiedTable
    , _tfirPCols :: [PGColInfo]
    }
  deriving (Show, Eq)
makePrisms ''TyFldInfo

type FieldMap
  = Map.HashMap (G.NamedType, G.Name) TyFldInfo

-- order by context
data OrdByItem
  = OBIPGCol !PGColInfo
  | OBIRel !RelInfo !AnnBoolExpSQL
  deriving (Show, Eq)

type OrdByItemMap = Map.HashMap G.Name OrdByItem

type OrdByCtx = Map.HashMap G.NamedType OrdByItemMap

-- insert context
type RelationInfoMap = Map.HashMap RelName RelInfo

data InsCtx
  = InsCtx
  { icView      :: !QualifiedTable
  , icColumns   :: ![PGColInfo]
  , icSet       :: !InsSetCols
  , icRelations :: !RelationInfoMap
  } deriving (Show, Eq)

type InsCtxMap = Map.HashMap QualifiedTable InsCtx
