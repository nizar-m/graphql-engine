{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Hasura.GraphQL.Validate.Field
  ( ArgsMap
  , Field(..)
  , SelSet
  , denormSelSet
  , mergeFields
  ) where

import           Data.Has
import           Hasura.Prelude

import qualified Data.Aeson                          as J
import qualified Data.Aeson.Casing                   as J
import qualified Data.Aeson.TH                       as J
import qualified Data.HashMap.Strict                 as Map
import qualified Data.HashMap.Strict.InsOrd.Extended as OMap
import qualified Data.List                           as L
import qualified Data.Sequence                       as Seq
import qualified Data.Sequence.NonEmpty              as NE
import qualified Data.Text                           as T
import qualified Language.GraphQL.Draft.Syntax       as G

import           Hasura.GraphQL.Validate.Context
import           Hasura.GraphQL.Validate.InputValue
import           Hasura.GraphQL.Validate.Types
import           Hasura.RQL.Types
import           Hasura.SQL.Value

-- data ScalarInfo
--   = SIBuiltin !GBuiltin
--   | SICustom !PGColType
--   deriving (Show, Eq)

-- data GBuiltin
--   = GInt
--   | GFloat
--   | GBoolean
--   | GString
--   deriving (Show, Eq)

data TypedOperation
  = TypedOperation
  { _toType         :: !G.OperationType
  , _toName         :: !(Maybe G.Name)
  , _toSelectionSet :: ![Field]
  } deriving (Show, Eq)

type ArgsMap = Map.HashMap G.Name AnnGValue

type CondSelSetMap = Map.HashMap G.NamedType SelSet

type SelSet = Seq.Seq Field

data Field
  = Field
  { _fAlias         :: !G.Alias
  , _fName          :: !G.Name
  , _fType          :: !G.NamedType
  , _fArguments     :: !ArgsMap
  , _fSelSet        :: !SelSet
  , _fCondSelSet    :: !CondSelSetMap
  } deriving (Eq, Show)

$(J.deriveToJSON (J.aesonDrop 2 J.camelCase){J.omitNothingFields=True}
  ''Field
 )

data FieldGroupSrc
  = FGSFragSprd !G.Name
  | FGSInlnFrag
  deriving (Show, Eq)

data FieldGroup
  = FieldGroup
  { _fgSource :: !FieldGroupSrc
  , _fgTyInfo :: !G.NamedType
  , _fgFields :: !(Seq.Seq Field)
  , _fgCondFields :: !(Map.HashMap G.NamedType (Seq.Seq Field))
  } deriving (Show, Eq)


--getTypeCondFieldsFG
--  :: (MonadReader r m, Has TypeMap r
--     , MonadError QErr m)
--  => ObjTyInfo
--  -> FieldGroup
--  -> m (Maybe (Seq.Seq Field))
--getTypeCondFieldsFG tyInfo fg = do
--  fgTyInfo <- getTyInfo $ _fgTyInfo fg
--  return $ getTypeCondFields' fgTyInfo (_fgFields fg) (_fgCondFields fg) tyInfo
--
--getTypeCondFieldsF
--  :: (MonadReader r m, Has TypeMap r
--     , MonadError QErr m)
--  => ObjTyInfo
--  -> Field
--  -> m (Maybe (Seq.Seq Field))
--getTypeCondFieldsF tyInfo f = do
--  fldTyInfo <- getTyInfo $ _fType f
--  return $ getTypeCondFields' fldTyInfo (_fSelSet f) (_fCondSelSet f) tyInfo
--
--getTypeCondFields' :: TypeInfo -> a -> Map.HashMap G.NamedType a -> ObjTyInfo -> Maybe a
--getTypeCondFields' srcTyInfo srcFlds srcCondFlds tyInfo
--  | TIObj tyInfo == srcTyInfo = Just srcFlds
--  | TIObj tyInfo `implmntsIFace` srcTyInfo = Just $ fromMaybe srcFlds $ Map.lookup (_otiName tyInfo) $ srcCondFlds
--  | otherwise = Nothing
--  where
--    implmntsIFace (TIObj o) (TIIFace i) = _ifName i `elem` _otiImplIfaces o
--    implmntsIFace _ _ = False


-- data GLoc
--   = GLoc
--   { _glLine   :: !Int
--   , _glColumn :: !Int
--   } deriving (Show, Eq)

-- data GErr
--   = GErr
--   { _geMessage   :: !Text
--   , _geLocations :: ![GLoc]
--   } deriving (Show, Eq)

-- throwGE :: (MonadError QErr m) => Text -> m a
-- throwGE msg = throwError $ QErr msg []

withDirectives
  :: ( MonadReader ValidationCtx m
     , MonadError QErr m)
  => [G.Directive]
  -> m a
  -> m (Maybe a)
withDirectives dirs act = do

  dirDefs <- onLeft (mkMapWith G._dName dirs) $ \dups ->
    throwVE $ "the following directives are used more than once: " <>
    showNames dups

  procDirs <- flip Map.traverseWithKey dirDefs $ \name dir ->
    withPathK (G.unName name) $ do
      dirInfo <- onNothing (Map.lookup (G._dName dir) defDirectivesMap) $
                 throwVE $ "unexpected directive: " <> showName name
      procArgs <- withPathK "args" $ processArgs (_diParams dirInfo)
                  (G._dArguments dir)
      getIfArg procArgs

  let shouldSkip    = fromMaybe False $ Map.lookup "skip" procDirs
      shouldInclude = fromMaybe True $ Map.lookup "include" procDirs

  if not shouldSkip && shouldInclude
    then Just <$> act
    else return Nothing

  where
    getIfArg m = do
      val <- onNothing (Map.lookup "if" m) $ throw500
              "missing if argument in the directive"
      case val of
        AGScalar _ (Just (PGValBoolean v)) -> return v
        _ -> throw500 "did not find boolean scalar for if argument"

denormSel
  :: ( MonadReader ValidationCtx m
     , MonadError QErr m)
  => [G.Name] -- visited fragments
  -> SelSetObj -- parent type info
  -> G.Selection
  -> m (Maybe (Either Field FieldGroup))
denormSel visFrags parTyInfo sel = case sel of
  G.SelectionField fld -> withPathK (G.unName $ G._fName fld) $ do
    fldInfo <- getFldInfo $ G._fName fld
    fmap Left <$> denormFld visFrags fldInfo fld
  G.SelectionFragmentSpread fragSprd ->
    withPathK (G.unName $ G._fsName fragSprd) $
    fmap Right <$> denormFrag visFrags parTyInfo fragSprd
  G.SelectionInlineFragment inlnFrag ->
    withPathK "inlineFragment" $
    fmap Right <$> denormInlnFrag visFrags parTyInfo inlnFrag
  where
    getFldInfo = case parTyInfo of
      SSOObj obj -> getFieldInfo obj
      SSOIFace i -> getIFaceFieldInfo i

processArgs
  :: ( MonadReader ValidationCtx m
     , MonadError QErr m)
  => ParamMap
  -> [G.Argument]
  -> m (Map.HashMap G.Name AnnGValue)
processArgs fldParams argsL = do

  args <- onLeft (mkMapWith G._aName argsL) $ \dups ->
    throwVE $ "the following arguments are defined more than once: " <>
    showNames dups

  let requiredParams = Map.filter (G.isNotNull . _iviType) fldParams

  inpArgs <- forM args $ \(G.Argument argName argVal) ->
    withPathK (G.unName argName) $ do
      argTy <- getArgTy argName
      validateInputValue valueParser argTy argVal

  forM_ requiredParams $ \argDef -> do
    let param = _iviName argDef
    onNothing (Map.lookup param inpArgs) $ throwVE $ mconcat
      [ "the required argument ", showName param, " is missing"]

  return inpArgs

  where
    getArgTy argName =
      onNothing (_iviType <$> Map.lookup argName fldParams) $ throwVE $
      "no such argument " <> showName argName <> " is expected"

denormFld
  :: ( MonadReader ValidationCtx m
     , MonadError QErr m)
  => [G.Name] -- visited fragments
  -> ObjFldInfo
  -> G.Field
  -> m (Maybe Field)
denormFld visFrags fldInfo (G.Field aliasM name args dirs selSet) = do

  let fldTy = _fiTy fldInfo
      fldBaseTy = getBaseTy fldTy

  fldTyInfo <- getTyInfo fldBaseTy

  argMap <- withPathK "args" $ processArgs (_fiParams fldInfo) args

  (fields, condFlds) <- case (fldTyInfo, selSet) of

    (TIObj _, [])  ->
      throwVE $ "field " <> showName name <> " of type "
      <> G.showGT fldTy <> " must have a selection of subfields"

    (TIObj fldObjTyInfo, _) ->
      denormSelSet visFrags (SSOObj fldObjTyInfo) selSet

    (TIIFace _, []) ->
      throwVE $ "field " <> showName name <> " of type "
      <> G.showGT fldTy <> " must have a selection of subfields"

    (TIIFace ifaceInfo, _) -> denormSelSet visFrags (SSOIFace ifaceInfo) selSet
    
    (TIScalar _, []) -> return (Seq.empty, Map.empty)
    (TIEnum _, []) -> return (Seq.empty, Map.empty)

    (TIInpObj _, _) ->
      throwVE $ "internal error: unexpected input type for field: "
      <> showName name

    -- when scalar/enum and no empty set
    (_, _) ->
      throwVE $ "field " <> showName name <> " must not have a "
      <> "selection since type " <> G.showGT fldTy <> " has no subfields"

  withPathK "directives" $ withDirectives dirs $ return $
    Field (fromMaybe (G.Alias name) aliasM) name fldBaseTy argMap fields condFlds

denormInlnFrag
  :: ( MonadReader ValidationCtx m
     , MonadError QErr m)
  => [G.Name] -- visited fragments
  -> SelSetObj -- type information of the field
  -> G.InlineFragment
  -> m (Maybe FieldGroup)
denormInlnFrag visFrags fldTyInfo inlnFrag = do
  fragTyInfo <- validateFragTypeCond fragTy fldTyInfo
  withPathK "directives" $ withDirectives directives $
    fmap (uncurry $ FieldGroup FGSInlnFrag fragTy) $
    denormSelSet visFrags fragTyInfo selSet
  where
    G.InlineFragment tyM directives selSet = inlnFrag
    fldTy  = selSetTyName fldTyInfo
    fragTy = fromMaybe fldTy tyM


validateFragTypeCond
  :: ( MonadReader ValidationCtx m
     , MonadError QErr m)
  => G.NamedType
  -> SelSetObj
  -> m SelSetObj
validateFragTypeCond fragTy fldTyInfo = do
  tyInfo :: TypeMap <- asks getter
  fragTyInfo <- getFragTyInfo
  let fragPossTy = getPossibleObjTy' fragTyInfo $ Map.elems tyInfo
      fldPossTy = getPossibleObjTy' fldTyInfo $ Map.elems tyInfo
      applTy = fragPossTy `L.intersect` fldPossTy
  when (null applTy) $ throwVE $ "Fragment cannot be spread as objects of the type " <>
    showNamedTy fldTy <> " can never be of fragment type " <> showNamedTy fragTy
  return fragTyInfo
  where
    fldTy = selSetTyName fldTyInfo
    getFragTyInfo = do
      tyMap <- asks _vcTypeMap
      case Map.lookup fragTy tyMap of
        Just (TIObj obj) -> return $ SSOObj obj
        Just (TIIFace i) -> return $ SSOIFace i
        Just (TIInpObj {}) -> invalidFragTy "an input object"
        Just (TIScalar {}) -> invalidFragTy "a scalar"
        Just (TIEnum {}) -> invalidFragTy "an enum"
        Nothing -> throwVE $ "Type '" <> showNamedTy fragTy <> "', defined as type condition on fragment for type: " <> showNamedTy fldTy <> ", is not found"
        where
          invalidFragTy tyStr = throwVE $ "Invalid Fragment type condition:  Type '" <> showNamedTy fragTy  <> "' is " <> tyStr

denormSelSet
  :: ( MonadReader ValidationCtx m
     , MonadError QErr m)
  => [G.Name] -- visited fragments
  -> SelSetObj
  -> G.SelectionSet
  -> m (Seq.Seq Field, CondSelSetMap)
denormSelSet visFrags fldTyInfo selSet =
  withPathK "selectionSet" $ do
    resFlds <- catMaybes <$> mapM (denormSel visFrags fldTyInfo) selSet
    let (defFlds, condFlds) = foldl' flatten (Seq.empty, Map.empty) resFlds
    flip (,) condFlds <$> mergeFields defFlds
  where
    fldTy = selSetTyName fldTyInfo
    flatten (s,cs) (Left fld) =  (s Seq.|> fld, fmap (Seq.|> fld) cs)
    flatten (s,cs) (Right (FieldGroup _ fragTy flds condFlds ))
      | fldTy == fragTy = addFldsAllSelSets (s,cs) (flds,condFlds) 
      | fldTyInfo `implmntsIFace` fragTy = (s Seq.>< fromMaybe flds (Map.lookup fldTy condFlds), cs)
      | otherwise               = (s, Map.alter (\v -> Just $ fromMaybe s v Seq.>< flds) fragTy cs )
      where
        addFldsAllSelSets (oSs, oCss) (nSs,nCss) =
          ( oSs Seq.>< nSs
          , Map.fromList $ fmap (\t -> (t, oFlds t Seq.>< nFlds t) ) allCondTy
          )
          where
            oFlds t = Map.lookupDefault oSs t oCss
            nFlds t = Map.lookupDefault nSs t nCss
            allCondTy = Map.keys nCss `L.union` Map.keys oCss
        implmntsIFace (SSOObj o) i = i `elem` _otiImplIfaces o
        implmntsIFace _ _ = False


mergeFields
  :: (MonadError QErr m)
  => Seq.Seq Field
  -> m (Seq.Seq Field)
mergeFields flds =
  fmap Seq.fromList $ forM fldGroups $ \fieldGroup -> do
    newFld <- checkMergeability fieldGroup
    childFields <- mergeFields $ foldl' (\l f -> l Seq.>< _fSelSet f) Seq.empty
                   $ NE.toSeq fieldGroup
    return $ newFld {_fSelSet = childFields}
  where
    fldGroups = OMap.elems $ OMap.groupListWith _fAlias flds
    -- can a group be merged?
    checkMergeability fldGroup = do
      let groupedFlds = toList $ NE.toSeq fldGroup
          fldNames = L.nub $ map _fName groupedFlds
          args = L.nub $ map _fArguments groupedFlds
          fld = NE.head fldGroup
          fldAl = _fAlias fld
      when (length fldNames > 1) $
        throwVE $ "cannot merge different fields under the same alias ("
        <> showName (G.unAlias fldAl) <> "): "
        <> showNames fldNames
      when (length args > 1) $
        throwVE $ "cannot merge fields with different arguments"
        <> " under the same alias: "
        <> showName (G.unAlias fldAl)
      return fld

denormFrag
  :: ( MonadReader ValidationCtx m
     , MonadError QErr m)
  => [G.Name] -- visited fragments
  -> SelSetObj -- parent type
  -> G.FragmentSpread
  -> m (Maybe FieldGroup)
denormFrag visFrags parTyInfo (G.FragmentSpread name directives) = do

  -- check for cycles
  when (name `elem` visFrags) $
    throwVE $ "cannot spread fragment " <> showName name
    <> " within itself via "
    <> T.intercalate "," (map G.unName visFrags)

  (FragDef _ fragTy selSet) <- getFragInfo

  -- Check whether there is any intersection between allowedTypes on the parent and fragment
  fragTyInfo <- validateFragTypeCond fragTy parTyInfo

  (resFlds, resCondFlds) <- denormSelSet (name:visFrags) fragTyInfo selSet

  withPathK "directives" $ withDirectives directives $
    return $ FieldGroup (FGSFragSprd  name) fragTy resFlds resCondFlds

  where
    getFragInfo = do
      dctx <- ask
      onNothing (Map.lookup name $ _vcFragDefMap dctx) $
        throwVE $ "fragment '" <> G.unName name <> "' not found"
