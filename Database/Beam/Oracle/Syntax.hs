{-# options_ghc -fno-warn-name-shadowing #-}
{-# language
    CPP
  , DeriveGeneric
  , FlexibleContexts
  , FlexibleInstances
  , GeneralizedNewtypeDeriving
  , LambdaCase
  , MultiParamTypeClasses
  , OverloadedStrings
  , RankNTypes
  , ScopedTypeVariables
  , TypeFamilies
#-}
module Database.Beam.Oracle.Syntax where

import           Database.Beam.Backend.SQL
import           Database.Beam.Query
import           Database.Beam.Query.SQL92
import           Database.Beam.Migrate.SQL.SQL92
import           Database.Beam.Migrate.Serialization
import           Database.Beam.Migrate.SQL.Builder hiding (fromSqlConstraintAttributes)

import           Control.Monad.State
import           Data.Coerce
import           Data.String
import           Data.ByteString.Builder
import           Data.ByteString.Builder.Scientific (scientificBuilder)
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.DList as DL
import           Data.Fixed
import           Data.Functor.Identity
import           Data.Hashable
import           Data.Int
import           Data.Monoid (Monoid)
import           Data.Scientific (Scientific)
import           Data.Semigroup (Semigroup, (<>))
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import           Data.Time
import           Data.Word
import qualified Database.Odpi as Odpi

data OraSyntax = OraSyntax (forall m. (Monad m) => (Odpi.NativeValue -> m Builder) -> m Builder) (DL.DList Odpi.NativeValue)

instance Show OraSyntax where
  show (OraSyntax s d) =
    "OraSyntax (" <> show (toLazyByteString (withPlaceholders s)) <> ") " <> show d

instance Eq OraSyntax where
  OraSyntax ab av == OraSyntax bb bv =
    toLazyByteString (withPlaceholders ab) == toLazyByteString (withPlaceholders bb)
    && av == bv

instance Semigroup OraSyntax where
  (OraSyntax ab av) <> (OraSyntax bb bv) =
    OraSyntax (\v -> (<>) <$> ab v <*> bb v) (av <> bv)

instance Monoid OraSyntax where
  mempty = OraSyntax (\_ -> pure mempty) mempty
  mappend = (<>)

instance Hashable OraSyntax where
  hashWithSalt salt (OraSyntax s d) =
    hashWithSalt salt ( toLazyByteString (withPlaceholders s)
                      , DL.toList d
                      )

newtype OraSelectSyntax = OraSelectSyntax { fromOraSelect :: OraSyntax }
newtype OraInsertSyntax = OraInsertSyntax { fromOraInsert :: OraSyntax }
newtype OraUpdateSyntax = OraUpdateSyntax { fromOraUpdate :: OraSyntax }
newtype OraDeleteSyntax = OraDeleteSyntax { fromOraDelete :: OraSyntax }
newtype OraFieldNameSyntax = OraFieldNameSyntax { fromOraFieldName :: OraSyntax }
newtype OraExpressionSyntax = OraExpressionSyntax { fromOraExpression :: OraSyntax } deriving Eq
newtype OraValueSyntax = OraValueSyntax { fromOraValue :: OraSyntax }
newtype OraInsertValuesSyntax = OraInsertValuesSyntax { fromOraInsertValues :: OraSyntax }
newtype OraSelectTableSyntax = OraSelectTableSyntax { fromOraSelectTable :: OraSyntax }
newtype OraSetQuantifierSyntax = OraSetQuantifierSyntax { fromOraSetQuantifier :: OraSyntax }
newtype OraComparisonQuantifierSyntax = OraComparisonQuantifierSyntax { fromOraComparisonQuantifier :: OraSyntax }
newtype OraOrderingSyntax = OraOrderingSyntax { fromOraOrdering :: OraSyntax }
newtype OraFromSyntax = OraFromSyntax { fromOraFrom :: OraSyntax }
newtype OraGroupingSyntax = OraGroupingSyntax { fromOraGrouping :: OraSyntax }
newtype OraTableSourceSyntax = OraTableSourceSyntax { fromOraTableSource :: OraSyntax }
newtype OraProjectionSyntax = OraProjectionSyntax { fromOraProjection :: OraSyntax }
newtype OraExtractFieldSyntax = OraExtractFieldSyntax { fromOraExtractField :: OraSyntax }

newtype OraColumnSchemaSyntax = OraColumnSchemaSyntax { fromOraColumnSchema :: OraSyntax } deriving (Eq)
instance Sql92DisplaySyntax OraColumnSchemaSyntax where
  displaySyntax = displaySyntax . fromOraColumnSchema
instance Hashable OraColumnSchemaSyntax where
  hashWithSalt salt = hashWithSalt salt . fromOraColumnSchema

newtype OraCreateTableSyntax = OraCreateTableSyntax { fromOraCreateTable :: OraSyntax } deriving (Eq)
newtype OraDropTableSyntax = OraDropTableSyntax { fromOraDropTable :: OraSyntax } deriving (Eq)
newtype OraAlterTableSyntax = OraAlterTableSyntax { fromOraAlterTable :: OraSyntax } deriving (Eq)
newtype OraAlterTableActionSyntax = OraAlterTableActionSyntax { fromOraAlterTableAction :: OraSyntax } deriving (Eq)
newtype OraAlterColumnActionSyntax = OraAlterColumnActionSyntax { fromOraAlterColumnAction :: OraSyntax } deriving (Eq)
data OraTableOptionsSyntax = OraTableOptionsSyntax OraSyntax OraSyntax

data OraDataTypeDescr
  = OraDataTypeDescrDomain Text
  | OraDataTypeDescr
  { oraTyName :: Text
  , oraTyLength :: Maybe Word
  , oraTyPrecision :: Maybe Word
  , oraTyScale :: Maybe Int
  } deriving (Show, Eq)
instance Hashable OraDataTypeDescr where
  hashWithSalt salt (OraDataTypeDescr nm len prec sc) =
    hashWithSalt salt (0 :: Int, nm, len, prec, sc)
  hashWithSalt salt (OraDataTypeDescrDomain t) =
    hashWithSalt salt (1 :: Int, t)

data OraDataTypeSyntax
  = OraDataTypeSyntax
  { oraDataTypeDescr :: OraDataTypeDescr
  , fromOraDataType :: OraSyntax
  , oraDataTypeSerialized :: BeamSerializedDataType
  }
instance Hashable OraDataTypeSyntax where
  hashWithSalt salt (OraDataTypeSyntax a _ _) = hashWithSalt salt a
instance Eq OraDataTypeSyntax where
  OraDataTypeSyntax a _ _ == OraDataTypeSyntax b _ _ = a == b

data OraMatchTypeSyntax
  = OraMatchTypeSyntax
  { fromOraMatchType :: OraSyntax
  , oraMatchTypeSerialized :: BeamSerializedMatchType
  }
data OraReferentialActionSyntax
  = OraReferentialActionSyntax
  { fromOraReferentialAction :: OraSyntax
  , oraReferentialActionSerialized :: BeamSerializedReferentialAction
  }

data OraColumnConstraintDefinitionSyntax
  = OraColumnConstraintDefinitionSyntax
  { fromOraColumnConstraintDefinition :: OraSyntax
  , oraColumnConstraintDefinitionSerialized :: BeamSerializedConstraintDefinition
  } deriving (Eq)
instance Hashable OraColumnConstraintDefinitionSyntax where
  hashWithSalt salt = hashWithSalt salt . fromOraColumnConstraintDefinition

instance Sql92DisplaySyntax OraColumnConstraintDefinitionSyntax where
  displaySyntax = displaySyntax . fromOraColumnConstraintDefinition

data OraColumnConstraintSyntax
  = OraColumnConstraintSyntax
  { fromOraColumnConstraint :: OraSyntax
  , oraColumnConstraintSerialized :: BeamSerializedConstraint
  }
newtype OraTableConstraintSyntax = OraTableConstraintSyntax { fromOraTableConstraint :: OraSyntax }

data OraCommandType
  = OraCommandTypeQuery
  | OraCommandTypeDdl
  | OraCommandTypeDataUpdate
  | OraCommandTypeDataUpdateReturning
  deriving Show

-- | Representation of an arbitrary Oracle command. This is the combination of
-- the command syntax (repesented by 'OraSyntax'), as well as the type of command
-- (represented by 'OraCommandType'). The command type is necessary for us to
-- know how to retrieve results from the database.
data OraCommandSyntax
  = OraCommandSyntax
  { oraCommandType :: OraCommandType
  , fromOraCommand :: OraSyntax
  }

-- | Convert the first argument of 'OraSyntax' to a 'ByteString' 'Builder',
-- where all the data has been replaced by @":x"@ placeholders.
withPlaceholders :: ((Odpi.NativeValue -> StateT Int Identity Builder) -> StateT Int Identity Builder) -> Builder
withPlaceholders build = runIdentity $ fmap fst $ runStateT (build $ \_ -> nextId) 1
  where
    nextId = do
      i <- get
      put $ i + 1
      pure $ ":" <> intDec i

emit :: Builder -> OraSyntax
emit b = OraSyntax (\_ -> pure b) mempty

-- | A best effort attempt to implement the escaping rules of Oracle. This is
-- never used to escape data sent to the database; only for emitting scripts or
-- displaying syntax to the user.
oraEscape :: T.Text -> T.Text
oraEscape = T.concatMap (\c -> if c == '"' then "\"\"" else T.singleton c)

oraIdentifier :: T.Text -> OraSyntax
oraIdentifier t =
  emit "\"" <> OraSyntax (\_ -> pure $ TE.encodeUtf8Builder (oraEscape t)) mempty <> emit "\""

emitValue :: Odpi.NativeValue -> OraSyntax
emitValue v = OraSyntax ($ v) (DL.singleton v)

-- | Render a 'SqliteSyntax' as a lazy 'BL.ByteString', for purposes of
-- displaying to a user. Embedded 'SQLData' is directly embedded into the
-- concrete syntax, with a best effort made to escape strings.
oraRenderSyntaxScript :: OraSyntax -> BL.ByteString
oraRenderSyntaxScript (OraSyntax s _) =
  error "TODO: oraRenderSyntaxScript"

oraSepBy :: OraSyntax -> [OraSyntax] -> OraSyntax
oraSepBy _ [] = mempty
oraSepBy _ [a] = a
oraSepBy sep (a:as) = a <> foldMap (sep <>) as

oraParens :: OraSyntax -> OraSyntax
oraParens a = emit "(" <> a <> emit ")"


------------------------------------------------------------------------------
-- Database.Beam.Backend.SQL.SQL92
------------------------------------------------------------------------------
instance IsSql92Syntax OraCommandSyntax where
    type Sql92SelectSyntax OraCommandSyntax = OraSelectSyntax
    type Sql92InsertSyntax OraCommandSyntax = OraInsertSyntax
    type Sql92UpdateSyntax OraCommandSyntax = OraUpdateSyntax
    type Sql92DeleteSyntax OraCommandSyntax = OraDeleteSyntax

    selectCmd = OraCommandSyntax OraCommandTypeQuery . fromOraSelect
    insertCmd = OraCommandSyntax OraCommandTypeDataUpdate . fromOraInsert
    deleteCmd = OraCommandSyntax OraCommandTypeDataUpdate . fromOraDelete
    updateCmd = OraCommandSyntax OraCommandTypeDataUpdate . fromOraUpdate

-- Select
instance IsSql92SelectSyntax OraSelectSyntax where
  type Sql92SelectSelectTableSyntax OraSelectSyntax = OraSelectTableSyntax
  type Sql92SelectOrderingSyntax OraSelectSyntax = OraOrderingSyntax

  -- Could be simplified with OFFSET and FETCH but we want to support pre 12.1
  -- See: https://blogs.oracle.com/oraclemagazine/on-rownum-and-limiting-results
  selectStmt tbl ordering limit offset =
    OraSelectSyntax $
    case (limit, offset) of
      (Just limit', Just offset') ->
          emit " SELECT * FROM (SELECT a.*, ROWNUM as rnum FROM ("
            <> sql <> emit ") a WHERE ROWNUM <= " <> emit (integerDec $ offset' + limit')
            <> emit ") WHERE rnum > " <> emit (integerDec offset')
      (Just limit', Nothing) ->
          emit " SELECT * FROM (" <> sql <> emit ") WHERE ROWNUM <= " <> emit (integerDec limit')
      (Nothing, Just offset') ->
          emit " SELECT * FROM (SELECT a.*, ROWNUM as rnum FROM ("
            <> sql <> emit ") a) WHERE rnum > " <> emit (integerDec offset')
      _ -> sql
    where
      sql = fromOraSelectTable tbl <>
            (case ordering of
              [] -> mempty
              _  -> emit " ORDER BY " <> oraSepBy (emit ", ") (map fromOraOrdering ordering))

instance IsSql92SelectTableSyntax OraSelectTableSyntax where
    type Sql92SelectTableSelectSyntax OraSelectTableSyntax = OraSelectSyntax
    type Sql92SelectTableExpressionSyntax OraSelectTableSyntax = OraExpressionSyntax
    type Sql92SelectTableProjectionSyntax OraSelectTableSyntax = OraProjectionSyntax
    type Sql92SelectTableFromSyntax OraSelectTableSyntax = OraFromSyntax
    type Sql92SelectTableGroupingSyntax OraSelectTableSyntax = OraGroupingSyntax
    type Sql92SelectTableSetQuantifierSyntax OraSelectTableSyntax = OraSetQuantifierSyntax

    selectTableStmt setQuantifier proj from where_ grouping having =
      OraSelectTableSyntax $
      emit "SELECT " <>
      maybe mempty (\sq' -> fromOraSetQuantifier sq' <> emit " ") setQuantifier <>
      fromOraProjection proj <>
      maybe mempty (emit " FROM " <>) (fmap fromOraFrom from) <>
      maybe mempty (emit " WHERE " <>) (fmap fromOraExpression where_) <>
      maybe mempty (emit " GROUP BY " <>) (fmap fromOraGrouping grouping) <>
      maybe mempty (emit " HAVING " <>) (fmap fromOraExpression having)

    unionTables True  = oraTblOp "UNION ALL"
    unionTables False = oraTblOp "UNION"
    intersectTables _ = oraTblOp "INTERSECT"
    exceptTable _ = oraTblOp "EXCEPT"

-- Insert
instance IsSql92InsertSyntax OraInsertSyntax where
    type Sql92InsertValuesSyntax OraInsertSyntax = OraInsertValuesSyntax

    insertStmt tblName fields values =
      OraInsertSyntax $
      emit "INSERT INTO " <> oraIdentifier tblName <> emit "(" <>
      oraSepBy (emit ", ") (map oraIdentifier fields) <> emit ")" <>
      fromOraInsertValues values

instance IsSql92InsertValuesSyntax OraInsertValuesSyntax where
    type Sql92InsertValuesExpressionSyntax OraInsertValuesSyntax = OraExpressionSyntax
    type Sql92InsertValuesSelectSyntax OraInsertValuesSyntax = OraSelectSyntax

    insertSqlExpressions es =
        OraInsertValuesSyntax $
        emit "VALUES " <>
        oraSepBy (emit ", ")
                 (map (\es' -> emit "(" <> oraSepBy (emit ", ") (fmap fromOraExpression es') <> emit ")")
                      es
                 )
    insertFromSql a = OraInsertValuesSyntax (fromOraSelect a)

-- Update
instance IsSql92UpdateSyntax OraUpdateSyntax where
    type Sql92UpdateFieldNameSyntax OraUpdateSyntax = OraFieldNameSyntax
    type Sql92UpdateExpressionSyntax OraUpdateSyntax = OraExpressionSyntax

    updateStmt tbl fields where_ =
      OraUpdateSyntax $
      emit "UPDATE " <> oraIdentifier tbl <>
      (case fields of
         [] -> mempty
         _ ->
           emit " SET " <>
           oraSepBy (emit ", ") (map (\(field, val) -> fromOraFieldName field <> emit "=" <>
                                                       fromOraExpression val) fields)) <>
      maybe mempty (\where' -> emit " WHERE " <> fromOraExpression where') where_

-- Delete
instance IsSql92DeleteSyntax OraDeleteSyntax where
    type Sql92DeleteExpressionSyntax OraDeleteSyntax = OraExpressionSyntax

    deleteStmt tbl alias where_ =
      OraDeleteSyntax $
      emit "DELETE FROM " <> oraIdentifier tbl <>
      maybe mempty (\alias_ -> emit " AS " <> oraIdentifier alias_) alias <>
      maybe mempty (\where' -> emit " WHERE " <> fromOraExpression where') where_

    deleteSupportsAlias _ = True

instance IsSql92FieldNameSyntax OraFieldNameSyntax where
    qualifiedField a b = OraFieldNameSyntax $ oraIdentifier a <> emit "." <> oraIdentifier b
    unqualifiedField b = OraFieldNameSyntax $ oraIdentifier b

instance IsSql92QuantifierSyntax OraComparisonQuantifierSyntax where
    quantifyOverAll = OraComparisonQuantifierSyntax (emit "ALL")
    quantifyOverAny = OraComparisonQuantifierSyntax (emit "ANY")

instance IsSql92DataTypeSyntax OraDataTypeSyntax where
  domainType nm
    = OraDataTypeSyntax (OraDataTypeDescrDomain nm)
                        (oraIdentifier nm)
                        (domainType nm)
  charType prec charSet
    = OraDataTypeSyntax (OraDataTypeDescr "CHAR" (fmap fromIntegral prec) Nothing Nothing)
                        (emit "CHAR" <> oraOptPrec prec <> oraOptCharSet charSet)
                        (charType prec charSet)
  varCharType prec charSet
    = OraDataTypeSyntax (OraDataTypeDescr "VARCHAR2" (fmap fromIntegral prec) Nothing Nothing)
                        (emit "VARCHAR2" <> oraOptPrec prec <> oraOptCharSet charSet)
                        (varCharType prec charSet)
  nationalCharType prec
    = OraDataTypeSyntax (OraDataTypeDescr "NCHAR" (fmap fromIntegral prec) Nothing Nothing)
                        (emit "NCHAR" <> oraOptPrec prec)
                        (nationalCharType prec)
  nationalVarCharType prec
    = OraDataTypeSyntax (OraDataTypeDescr "NVARCHAR2" (fmap fromIntegral prec) Nothing Nothing)
                        (emit "NVARCHAR2" <> oraOptPrec prec)
                        (nationalVarCharType prec)
  bitType _
    = error "TODO: bitType"
      -- OraDataTypeSyntax (OraDataTypeDescr "CHAR" Nothing (fmap fromIntegral prec) Nothing)
      --                   (emit "CHAR" <> oraOptPrec prec)
      --                   (bitType prec)
  varBitType _
    = error "TODO: varBitType"
      -- OraDataTypeSyntax (OraDataTypeDescr (Pg.typoid Pg.varbit) (fmap fromIntegral prec))
      --                   (emit "BIT VARYING" <> pgOptPrec prec)
      --                   (varBitType prec)
  numericType Nothing
    = OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing Nothing Nothing)
                        (emit "NUMBER" <> oraOptNumericPrec Nothing)
                        (numericType Nothing)
  numericType prec@(Just (p, s))
    = OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing (Just p) (fmap fromIntegral s))
                        (emit "NUMBER" <> oraOptNumericPrec prec)
                        (numericType prec)
  decimalType
    = numericType
  intType
    = OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing (Just 10) (Just 0))
                        (emit "NUMBER" <> oraOptNumericPrec (Just (10, Just 0)))
                        intType
  smallIntType
    = OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing (Just 5) (Just 0))
                        (emit "NUMBER" <> oraOptNumericPrec (Just (5, Just 0)))
                        smallIntType
  floatType _
    = OraDataTypeSyntax (OraDataTypeDescr "BINARY_FLOAT" Nothing Nothing Nothing)
                        (emit "BINARY_FLOAT")
                        (floatType Nothing)
  doubleType
    = OraDataTypeSyntax (OraDataTypeDescr "BINARY_DOUBLE" Nothing Nothing Nothing)
                        (emit "BINARY_DOUBLE")
                        doubleType
  realType
    = error "TODO: realType?"
  dateType
    = OraDataTypeSyntax (OraDataTypeDescr "DATE" Nothing Nothing Nothing)
                        (emit "DATE")
                        dateType
  --timeType prec withTz
  timeType _ _
    = error "TODO: timeType"
  timestampType prec withTz
    = OraDataTypeSyntax (OraDataTypeDescr "TIMESTAMP" Nothing Nothing Nothing)
                        (emit "TIMESTAMP" <> oraOptPrec prec <> if withTz then emit " WITH TIME ZONE" else mempty)
                        (timestampType prec withTz)


instance IsSql92ExpressionSyntax OraExpressionSyntax where
    type Sql92ExpressionValueSyntax OraExpressionSyntax = OraValueSyntax
    type Sql92ExpressionSelectSyntax OraExpressionSyntax = OraSelectSyntax
    type Sql92ExpressionFieldNameSyntax OraExpressionSyntax = OraFieldNameSyntax
    type Sql92ExpressionQuantifierSyntax OraExpressionSyntax = OraComparisonQuantifierSyntax
    type Sql92ExpressionCastTargetSyntax OraExpressionSyntax = OraDataTypeSyntax
    type Sql92ExpressionExtractFieldSyntax OraExpressionSyntax = OraExtractFieldSyntax

    addE = oraBinOp "+"
    subE = oraBinOp "-"
    mulE = oraBinOp "*"
    divE = oraBinOp "/"
    modE = oraBinOp "%"

    orE = oraBinOp "OR"
    andE = oraBinOp "AND"
    likeE = oraBinOp "LIKE"
    overlapsE = oraBinOp "OVERLAPS"

    eqE = oraCompOp "="
    neqE = oraCompOp "<>"
    ltE = oraCompOp "<"
    gtE = oraCompOp ">"
    leE = oraCompOp "<="
    geE = oraCompOp ">="

    negateE = oraUnOp "-"
    notE = oraUnOp "NOT"

    eqMaybeE a b e = (isNullE a `andE` isNullE b) `orE` e
    neqMaybeE a b e = notE $ eqMaybeE a b (notE e)
    --neqMaybeE a b e = ((isNullE a `andE` isNotNullE b) `orE` (isNotNullE a `andE` isNullE b)) `orE` e

    existsE s = OraExpressionSyntax $ emit "EXISTS(" <> fromOraSelect s <> emit ")"
    uniqueE s = OraExpressionSyntax $ emit "UNIQUE(" <> fromOraSelect s <> emit ")"

    isNotNullE = oraPostFix "IS NOT NULL"
    isNullE = oraPostFix "IS NULL"
    isTrueE = oraPostFix "IS TRUE"
    isFalseE = oraPostFix "IS FALSE"
    isNotTrueE = oraPostFix "IS NOT TRUE"
    isNotFalseE = oraPostFix "IS NOT FALSE"
    isUnknownE = oraPostFix "IS UNKNOWN"
    isNotUnknownE = oraPostFix "IS NOT UNKNOWN"

    betweenE a b c =
        OraExpressionSyntax (emit "(" <> fromOraExpression a <> emit ") BETWEEN (" <>
                               fromOraExpression b <> emit ") AND (" <>
                               fromOraExpression c <> emit ")")

    valueE e = OraExpressionSyntax (fromOraValue e)
    rowE vs = OraExpressionSyntax (emit "(" <> oraSepBy (emit ", ") (map fromOraExpression vs) <> emit ")")
    fieldE fn = OraExpressionSyntax (fromOraFieldName fn)
    subqueryE s = OraExpressionSyntax (emit "(" <> fromOraSelect s <> emit ")")

    positionE needle haystack =
        OraExpressionSyntax $
        emit "POSITION((" <> fromOraExpression needle <> emit ") IN ("
                          <> fromOraExpression haystack <> emit "))"

    nullIfE a b =
        OraExpressionSyntax $
        emit "NULLIF(" <> fromOraExpression a <> emit ", " <>
        fromOraExpression b <> emit ")"

    absE a = OraExpressionSyntax (emit "ABS(" <> fromOraExpression a <> emit ")")
    bitLengthE a = OraExpressionSyntax (emit "BIT_LENGTH(" <> fromOraExpression a <> emit ")")
    charLengthE a = OraExpressionSyntax (emit "CHAR_LENGTH(" <> fromOraExpression a <> emit ")")
    octetLengthE a = OraExpressionSyntax (emit "OCTET_LENGTH(" <> fromOraExpression a <> emit ")")
    coalesceE es = OraExpressionSyntax (emit "COALESCE("
                                        <> oraSepBy (emit ", ") (map fromOraExpression es)
                                        <> emit ")")
    extractE field from = OraExpressionSyntax (emit "EXTRACT(" <> fromOraExtractField field <>
                                                 emit " FROM (" <> fromOraExpression from <> emit ")")
    castE e to = OraExpressionSyntax (emit "CAST(" <> fromOraExpression e <> emit ") AS " <>
                                        fromOraDataType to <> emit ")")
    caseE cases else' =
        OraExpressionSyntax $
        emit "CASE " <>
        foldMap (\(cond, res) -> emit "WHEN " <> fromOraExpression cond <>
                                 emit " THEN " <> fromOraExpression res <>
                                 emit " ") cases <>
        emit "ELSE " <> fromOraExpression else' <> emit " END"

    currentTimestampE = OraExpressionSyntax (emit "CURRENT_TIMESTAMP")
    defaultE = OraExpressionSyntax (emit "DEFAULT")

    inE e es = OraExpressionSyntax $
               emit "(" <> fromOraExpression e <> emit ") IN ( " <>
               oraSepBy (emit ", ") (map fromOraExpression es) <> emit ")"

    trimE x = OraExpressionSyntax (emit "TRIM(" <> fromOraExpression x <> emit ")")
    lowerE x = OraExpressionSyntax (emit "LOWER(" <> fromOraExpression x <> emit ")")
    upperE x = OraExpressionSyntax (emit "UPPER(" <> fromOraExpression x <> emit ")")

instance IsSql92AggregationExpressionSyntax OraExpressionSyntax where
    type Sql92AggregationSetQuantifierSyntax OraExpressionSyntax = OraSetQuantifierSyntax

    countAllE = OraExpressionSyntax (emit "COUNT(*)")
    countE = oraUnAgg "COUNT"
    avgE = oraUnAgg "AVG"
    sumE = oraUnAgg "SUM"
    minE = oraUnAgg "MIN"
    maxE = oraUnAgg "MAX"

instance IsSql92AggregationSetQuantifierSyntax OraSetQuantifierSyntax where
    setQuantifierDistinct = OraSetQuantifierSyntax (emit "DISTINCT")
    setQuantifierAll = OraSetQuantifierSyntax (emit "ALL")

instance IsSql92ProjectionSyntax OraProjectionSyntax where
    type Sql92ProjectionExpressionSyntax OraProjectionSyntax = OraExpressionSyntax

    projExprs exprs =
        OraProjectionSyntax $
        oraSepBy (emit ", ")
                   (map (\(expr, nm) ->
                             fromOraExpression expr <>
                             maybe mempty
                                   (\nm' -> emit " AS " <> oraIdentifier nm') nm)
                        exprs)

instance IsSql92OrderingSyntax OraOrderingSyntax where
    type Sql92OrderingExpressionSyntax OraOrderingSyntax = OraExpressionSyntax

    ascOrdering e = OraOrderingSyntax (fromOraExpression e <> emit " ASC")
    descOrdering e = OraOrderingSyntax (fromOraExpression e <> emit " DESC")

instance IsSql92TableSourceSyntax OraTableSourceSyntax where
    type Sql92TableSourceSelectSyntax OraTableSourceSyntax = OraSelectSyntax

    tableNamed t = OraTableSourceSyntax (oraIdentifier t)
    tableFromSubSelect s = OraTableSourceSyntax (emit "(" <> fromOraSelect s <> emit ")")

instance IsSql92GroupingSyntax OraGroupingSyntax where
    type Sql92GroupingExpressionSyntax OraGroupingSyntax = OraExpressionSyntax

    groupByExpressions es =
      OraGroupingSyntax $
      oraSepBy (emit ", ") (map fromOraExpression es)

instance IsSql92FromSyntax OraFromSyntax where
    type Sql92FromExpressionSyntax OraFromSyntax = OraExpressionSyntax
    type Sql92FromTableSourceSyntax OraFromSyntax = OraTableSourceSyntax

    fromTable tableSrc Nothing = OraFromSyntax (fromOraTableSource tableSrc)
    fromTable tableSrc (Just nm) =
        OraFromSyntax $
        fromOraTableSource tableSrc <> emit " " <> oraIdentifier nm

    innerJoin = oraJoin "JOIN"

    leftJoin = oraJoin "LEFT JOIN"
    rightJoin = oraJoin "RIGHT JOIN"

instance IsSql92FromOuterJoinSyntax OraFromSyntax where
    outerJoin = oraJoin "OUTER JOIN"


------------------------------------------------------------------------------
-- Database.Beam.Backend.SQL.SQL99
------------------------------------------------------------------------------

-- TODO
-- instance IsSql99ExpressionSyntax expr where
--   distinctE :: Sql92ExpressionSelectSyntax expr -> expr
--   similarToE :: expr -> expr -> expr
--   functionCallE :: expr -> [ expr ] -> expr
--   instanceFieldE :: expr -> Text -> expr
--   refFieldE :: expr -> Text -> expr

instance IsSql99ConcatExpressionSyntax OraExpressionSyntax where
    concatE [] = valueE (sqlValueSyntax ("" :: T.Text))
    concatE xs =
        OraExpressionSyntax . mconcat $
        [ emit "CONCAT("
        , oraSepBy (emit ", ") (map fromOraExpression xs)
        , emit ")" ]

-- TODO
-- instance IsSql99AggregationExpressionSyntax OraExpressionSyntax where
--   everyE, someE, anyE :: Maybe (Sql92AggregationSetQuantifierSyntax expr) -> expr -> expr

instance IsSql99DataTypeSyntax OraDataTypeSyntax where
  characterLargeObjectType
    = OraDataTypeSyntax (OraDataTypeDescr "CLOB" Nothing Nothing Nothing)
                        (emit "CLOB")
                        characterLargeObjectType
  binaryLargeObjectType
    = OraDataTypeSyntax (OraDataTypeDescr "BLOB" Nothing Nothing Nothing)
                        (emit "BLOB")
                        binaryLargeObjectType
  booleanType
    = error "TODO: booleanType"
  arrayType -- :: dataType -> Int -> dataType
    = error "TODO: arrayType"
  rowType -- :: [ (Text, dataType) ] -> dataType
    = error "TODO: rowType"


------------------------------------------------------------------------------
-- Database.Beam.Backend.SQL.SQL2003
--
-- TODO: This whole section needs work
------------------------------------------------------------------------------

-- instance IsSql2003FromSyntax OraFromSyntax where
--     type Sql2003FromSampleMethodSyntax OraFromSyntax = 
--     fromTableSample :: Sql92FromTableSourceSyntax from
--                     -> Sql2003FromSampleMethodSyntax from
--                     -> Maybe Double
--                     -> Maybe Integer
--                     -> Maybe Text
--                     -> from

--instance IsSql2003OrderingElementaryOLAPOperationsSyntax ord where
--    nullsFirstOrdering, nullsLastOrdering :: ord -> ord

-- instance IsSql2003ExpressionSyntax OraExpressionSyntax where
--     type Sql2003ExpressionWindowFrameSyntax expr :: *
--     overE :: expr
--           -> Sql2003ExpressionWindowFrameSyntax expr
--           -> expr

-- instance IsSql2003EnhancedNumericFunctionsExpressionSyntax OraExpressionSyntax where
--     lnE    x = OraExpressionSyntax (emit "LN("    <> fromOraExpression x <> emit ")")
--     expE   x = OraExpressionSyntax (emit "EXP("   <> fromOraExpression x <> emit ")")
--     sqrtE  x = OraExpressionSyntax (emit "SQRT("  <> fromOraExpression x <> emit ")")
--     ceilE  x = OraExpressionSyntax (emit "CEIL("  <> fromOraExpression x <> emit ")")
--     floorE x = OraExpressionSyntax (emit "FLOOR(" <> fromOraExpression x <> emit ")")
--     powerE x y = OraExpressionSyntax (emit "POWER(" <> fromOraExpression x <> emit ", " <>
--                                         fromOraExpression y <> emit ")")

instance IsSql2008BigIntDataTypeSyntax OraDataTypeSyntax where
  bigIntType = OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing (Just 19) (Just 0))
                                 (emit "NUMBER" <> oraOptNumericPrec (Just (19, Just 0)))
                                 (numericType (Just (19, Just 0)))

------------------------------------------------------------------------------
-- Database.Beam.Migrate.SQL.SQL92
------------------------------------------------------------------------------
instance Sql92DisplaySyntax OraSyntax where
  displaySyntax = BL.unpack . oraRenderSyntaxScript

instance Sql92DisplaySyntax OraDataTypeSyntax where
  displaySyntax = displaySyntax . fromOraDataType

instance IsSql92DdlCommandSyntax OraCommandSyntax where
  type Sql92DdlCommandCreateTableSyntax OraCommandSyntax = OraCreateTableSyntax
  type Sql92DdlCommandDropTableSyntax OraCommandSyntax = OraDropTableSyntax
  type Sql92DdlCommandAlterTableSyntax OraCommandSyntax = OraAlterTableSyntax

  createTableCmd = OraCommandSyntax OraCommandTypeDdl . coerce
  dropTableCmd   = OraCommandSyntax OraCommandTypeDdl . coerce
  alterTableCmd  = OraCommandSyntax OraCommandTypeDdl . coerce

instance IsSql92CreateTableSyntax OraCreateTableSyntax where
  type Sql92CreateTableColumnSchemaSyntax OraCreateTableSyntax = OraColumnSchemaSyntax
  type Sql92CreateTableTableConstraintSyntax OraCreateTableSyntax = OraTableConstraintSyntax
  type Sql92CreateTableOptionsSyntax OraCreateTableSyntax = OraTableOptionsSyntax

  createTableSyntax options tblNm fieldTypes constraints =
    let (beforeOptions, afterOptions) =
          case options of
            Nothing -> (emit " ", emit " ")
            Just (OraTableOptionsSyntax before after) ->
              ( emit " " <> before <> emit " "
              , emit " " <> after <> emit " " )
    in OraCreateTableSyntax $
       emit "CREATE" <> beforeOptions <> emit "TABLE " <> oraIdentifier tblNm <>
       emit " (" <>
       oraSepBy (emit ", ")
               (map (\(nm, type_) -> oraIdentifier nm <> emit " " <> fromOraColumnSchema type_)  fieldTypes <>
                map fromOraTableConstraint constraints)
       <> emit ")" <> afterOptions

instance IsSql92DropTableSyntax OraDropTableSyntax where
  dropTableSyntax tblNm = OraDropTableSyntax $ emit "DROP TABLE " <> oraIdentifier tblNm

instance IsSql92AlterTableSyntax OraAlterTableSyntax where
  type Sql92AlterTableAlterTableActionSyntax OraAlterTableSyntax = OraAlterTableActionSyntax

  alterTableSyntax tblNm action =
    OraAlterTableSyntax $
    emit "ALTER TABLE " <> oraIdentifier tblNm <> emit " " <> fromOraAlterTableAction action

instance IsSql92AlterTableActionSyntax OraAlterTableActionSyntax where
  type Sql92AlterTableAlterColumnActionSyntax OraAlterTableActionSyntax = OraAlterColumnActionSyntax
  type Sql92AlterTableColumnSchemaSyntax OraAlterTableActionSyntax = OraColumnSchemaSyntax

  alterColumnSyntax colNm action =
    OraAlterTableActionSyntax $
    emit "ALTER COLUMN " <> oraIdentifier colNm <> emit " " <> fromOraAlterColumnAction action

  addColumnSyntax colNm schema =
    OraAlterTableActionSyntax $
    emit "ADD COLUMN " <> oraIdentifier colNm <> emit " " <> fromOraColumnSchema schema

  dropColumnSyntax colNm =
    OraAlterTableActionSyntax $
    emit "DROP COLUMN " <> oraIdentifier colNm

  renameTableToSyntax newNm =
    OraAlterTableActionSyntax $
    emit "RENAME TO " <> oraIdentifier newNm

  renameColumnToSyntax oldNm newNm =
    OraAlterTableActionSyntax $
    emit "RENAME COLUMN " <> oraIdentifier oldNm <> emit " TO " <> oraIdentifier newNm

instance IsSql92AlterColumnActionSyntax OraAlterColumnActionSyntax where
  setNullSyntax = OraAlterColumnActionSyntax $ emit "DROP NOT NULL"
  setNotNullSyntax = OraAlterColumnActionSyntax $ emit "SET NOT NULL"

instance IsSql92ColumnSchemaSyntax OraColumnSchemaSyntax where
  type Sql92ColumnSchemaColumnTypeSyntax OraColumnSchemaSyntax = OraDataTypeSyntax
  type Sql92ColumnSchemaExpressionSyntax OraColumnSchemaSyntax = OraExpressionSyntax
  type Sql92ColumnSchemaColumnConstraintDefinitionSyntax OraColumnSchemaSyntax = OraColumnConstraintDefinitionSyntax

  columnSchemaSyntax colType defaultClause constraints collation =
    OraColumnSchemaSyntax syntax
    where
      syntax =
        fromOraDataType colType <>
        maybe mempty (\d -> emit " DEFAULT " <> fromOraExpression d) defaultClause <>
        (case constraints of
           [] -> mempty
           _ -> foldMap (\c -> emit " " <> fromOraColumnConstraintDefinition c) constraints) <>
        maybe mempty (\nm -> emit " COLLATE " <> oraIdentifier nm) collation

instance IsSql92TableConstraintSyntax OraTableConstraintSyntax where
  primaryKeyConstraintSyntax fieldNames =
    OraTableConstraintSyntax $
    emit "PRIMARY KEY(" <> oraSepBy (emit ", ") (map oraIdentifier fieldNames) <> emit ")"

instance IsSql92MatchTypeSyntax OraMatchTypeSyntax where
  fullMatchSyntax = OraMatchTypeSyntax (emit "FULL") fullMatchSyntax
  partialMatchSyntax = OraMatchTypeSyntax (emit "PARTIAL") partialMatchSyntax

instance IsSql92ReferentialActionSyntax OraReferentialActionSyntax where
  referentialActionCascadeSyntax = OraReferentialActionSyntax (emit "CASCADE") referentialActionCascadeSyntax
  referentialActionNoActionSyntax = OraReferentialActionSyntax (emit "NO ACTION") referentialActionNoActionSyntax
  referentialActionSetDefaultSyntax = OraReferentialActionSyntax (emit "SET DEFAULT") referentialActionSetDefaultSyntax
  referentialActionSetNullSyntax = OraReferentialActionSyntax (emit "SET NULL") referentialActionSetNullSyntax

instance IsSql92ColumnConstraintDefinitionSyntax OraColumnConstraintDefinitionSyntax where
  type Sql92ColumnConstraintDefinitionConstraintSyntax OraColumnConstraintDefinitionSyntax = OraColumnConstraintSyntax
  type Sql92ColumnConstraintDefinitionAttributesSyntax OraColumnConstraintDefinitionSyntax = SqlConstraintAttributesBuilder

  constraintDefinitionSyntax nm constraint attrs =
    OraColumnConstraintDefinitionSyntax syntax
      (constraintDefinitionSyntax nm (oraColumnConstraintSerialized constraint) (fmap sqlConstraintAttributesSerialized attrs))
    where
      syntax =
        maybe mempty (\nm' -> emit "CONSTRAINT " <> oraIdentifier nm' <> emit " " ) nm <>
        fromOraColumnConstraint constraint <>
        maybe mempty (\a -> emit " " <> fromSqlConstraintAttributes a) attrs

instance IsSql92ColumnConstraintSyntax OraColumnConstraintSyntax where
  type Sql92ColumnConstraintMatchTypeSyntax OraColumnConstraintSyntax = OraMatchTypeSyntax
  type Sql92ColumnConstraintReferentialActionSyntax OraColumnConstraintSyntax = OraReferentialActionSyntax
  type Sql92ColumnConstraintExpressionSyntax OraColumnConstraintSyntax = OraExpressionSyntax

  notNullConstraintSyntax = OraColumnConstraintSyntax (emit "NOT NULL") notNullConstraintSyntax
  uniqueColumnConstraintSyntax = OraColumnConstraintSyntax (emit "UNIQUE") uniqueColumnConstraintSyntax
  primaryKeyColumnConstraintSyntax = OraColumnConstraintSyntax (emit "PRIMARY KEY") primaryKeyColumnConstraintSyntax
  checkColumnConstraintSyntax expr =
    OraColumnConstraintSyntax (emit "CHECK(" <> fromOraExpression expr <> emit ")")
                             (checkColumnConstraintSyntax . BeamSerializedExpression . TE.decodeUtf8 .
                              BL.toStrict . oraRenderSyntaxScript . fromOraExpression $ expr)
  referencesConstraintSyntax tbl fields matchType onUpdate onDelete =
    OraColumnConstraintSyntax syntax
      (referencesConstraintSyntax tbl fields (fmap oraMatchTypeSerialized matchType)
                                  (fmap oraReferentialActionSerialized onUpdate)
                                  (fmap oraReferentialActionSerialized onDelete))
    where
      syntax =
        emit "REFERENCES " <> oraIdentifier tbl <> emit "("
        <> oraSepBy (emit ", ") (map oraIdentifier fields) <> emit ")" <>
        maybe mempty (\m -> emit " " <> fromOraMatchType m) matchType <>
        maybe mempty (\a -> emit " ON UPDATE " <> fromOraReferentialAction a) onUpdate <>
        maybe mempty (\a -> emit " ON DELETE " <> fromOraReferentialAction a) onDelete

instance Sql92SerializableDataTypeSyntax OraDataTypeSyntax where
  serializeDataType = fromBeamSerializedDataType . oraDataTypeSerialized

instance Sql92SerializableConstraintDefinitionSyntax OraColumnConstraintDefinitionSyntax where
  serializeConstraint = fromBeamSerializedConstraintDefinition . oraColumnConstraintDefinitionSerialized


------------------------------------------------------------------------------
-- Database.Beam.Query.Types
------------------------------------------------------------------------------
instance HasQBuilder OraSelectSyntax where
    buildSqlQuery = buildSql92Query' True

oraTblOp :: Builder -> OraSelectTableSyntax -> OraSelectTableSyntax -> OraSelectTableSyntax
oraTblOp op a b =
    OraSelectTableSyntax (fromOraSelectTable a <> emit " " <> emit op <>
                            emit " " <> fromOraSelectTable b)


oraJoin :: Builder -> OraFromSyntax -> OraFromSyntax
          -> Maybe OraExpressionSyntax -> OraFromSyntax
oraJoin joinType a b (Just e) =
    OraFromSyntax (fromOraFrom a <> emit " " <> emit joinType <> emit " " <>
                     fromOraFrom b <> emit " ON " <> fromOraExpression e)
oraJoin joinType a b Nothing =
    OraFromSyntax (fromOraFrom a <> emit " " <> emit joinType <>
                     emit " " <> fromOraFrom b)

instance IsCustomSqlSyntax OraExpressionSyntax where
    newtype CustomSqlSyntax OraExpressionSyntax =
        OraCustomExpressionSyntax { fromOraCustomExpression :: OraSyntax }
        deriving (Monoid, Semigroup)
    customExprSyntax = OraExpressionSyntax . fromOraCustomExpression
    renderSyntax = OraCustomExpressionSyntax . oraParens . fromOraExpression

instance IsString (CustomSqlSyntax OraExpressionSyntax) where
    fromString = OraCustomExpressionSyntax . emit . fromString

oraUnOp :: Builder -> OraExpressionSyntax -> OraExpressionSyntax
oraUnOp op e = OraExpressionSyntax $ emit op <> emit " (" <> fromOraExpression e <> emit ")"

oraPostFix :: Builder -> OraExpressionSyntax -> OraExpressionSyntax
oraPostFix op e = OraExpressionSyntax $ emit "(" <> fromOraExpression e <> emit ") " <> emit op

oraCompOp :: Builder -> Maybe OraComparisonQuantifierSyntax
            -> OraExpressionSyntax -> OraExpressionSyntax
            -> OraExpressionSyntax
oraCompOp op quantifier a b =
    OraExpressionSyntax $
    emit "(" <> fromOraExpression a <>
    emit ") " <> emit op <>
    maybe mempty (\q -> emit " " <> fromOraComparisonQuantifier q <> emit " ") quantifier <>
    emit " (" <> fromOraExpression b <> emit ")"

oraBinOp :: Builder -> OraExpressionSyntax -> OraExpressionSyntax -> OraExpressionSyntax
oraBinOp op a b =
  OraExpressionSyntax $
  emit "(" <> fromOraExpression a <> emit ") " <> emit op <>
  emit " (" <> fromOraExpression b <> emit ")"

instance IsSqlExpressionSyntaxStringType OraExpressionSyntax String
instance IsSqlExpressionSyntaxStringType OraExpressionSyntax Text

oraUnAgg :: Builder -> Maybe OraSetQuantifierSyntax
           -> OraExpressionSyntax -> OraExpressionSyntax
oraUnAgg fn q e =
    OraExpressionSyntax $
    emit fn <> emit "(" <>
    maybe mempty (\q' -> fromOraSetQuantifier q' <> emit " ") q <>
    fromOraExpression e <> emit ")"

instance HasSqlValueSyntax OraValueSyntax SqlNull where
  sqlValueSyntax _ = OraValueSyntax $ emit "NULL"
instance HasSqlValueSyntax OraValueSyntax Bool where
  sqlValueSyntax True  = OraValueSyntax $ emit "TRUE"
  sqlValueSyntax False = OraValueSyntax $ emit "FALSE"

instance HasSqlValueSyntax OraValueSyntax Double where
  sqlValueSyntax d = OraValueSyntax $ emit (doubleDec d)
instance HasSqlValueSyntax OraValueSyntax Float where
  sqlValueSyntax d = OraValueSyntax $ emit (floatDec d)

instance HasSqlValueSyntax OraValueSyntax Int where
  sqlValueSyntax d = OraValueSyntax $ emit (intDec d)
instance HasSqlValueSyntax OraValueSyntax Int8 where
  sqlValueSyntax d = OraValueSyntax $ emit (int8Dec d)
instance HasSqlValueSyntax OraValueSyntax Int16 where
  sqlValueSyntax d = OraValueSyntax $ emit (int16Dec d)
instance HasSqlValueSyntax OraValueSyntax Int32 where
  sqlValueSyntax d = OraValueSyntax $ emit (int32Dec d)
instance HasSqlValueSyntax OraValueSyntax Int64 where
  sqlValueSyntax d = OraValueSyntax $ emit (int64Dec d)
instance HasSqlValueSyntax OraValueSyntax Integer where
  sqlValueSyntax d = OraValueSyntax $ emit (integerDec d)

instance HasSqlValueSyntax OraValueSyntax Word where
  sqlValueSyntax d = OraValueSyntax $ emit (wordDec d)
instance HasSqlValueSyntax OraValueSyntax Word8 where
  sqlValueSyntax d = OraValueSyntax $ emit (word8Dec d)
instance HasSqlValueSyntax OraValueSyntax Word16 where
  sqlValueSyntax d = OraValueSyntax $ emit (word16Dec d)
instance HasSqlValueSyntax OraValueSyntax Word32 where
  sqlValueSyntax d = OraValueSyntax $ emit (word32Dec d)
instance HasSqlValueSyntax OraValueSyntax Word64 where
  sqlValueSyntax d = OraValueSyntax $ emit (word64Dec d)

instance HasSqlValueSyntax OraValueSyntax T.Text where
  sqlValueSyntax t = OraValueSyntax $ emitValue $ Odpi.NativeBytes $ TE.encodeUtf8 t
instance HasSqlValueSyntax OraValueSyntax TL.Text where
  sqlValueSyntax = sqlValueSyntax . TL.toStrict
instance HasSqlValueSyntax OraValueSyntax [Char] where
  sqlValueSyntax = sqlValueSyntax . T.pack

instance HasSqlValueSyntax OraValueSyntax Scientific where
  sqlValueSyntax = OraValueSyntax . emit . scientificBuilder

instance HasSqlValueSyntax OraValueSyntax Day where
  sqlValueSyntax d = OraValueSyntax (emit ("'" <> dayBuilder d <> "'"))

instance HasSqlValueSyntax OraValueSyntax TimeOfDay where
  sqlValueSyntax d = OraValueSyntax (emit ("'" <> todBuilder d <> "'"))

dayBuilder :: Day -> Builder
dayBuilder d =
    integerDec year <> "-" <>
    (if month < 10 then "0" else mempty) <> intDec month <> "-" <>
    (if day   < 10 then "0" else mempty) <> intDec day
  where
    (year, month, day) = toGregorian d

todBuilder :: TimeOfDay -> Builder
todBuilder d =
    (if todHour d < 10 then "0" else mempty) <> intDec (todHour d) <> ":" <>
    (if todMin  d < 10 then "0" else mempty) <> intDec (todMin  d) <> ":" <>
    (if secs6 < 10 then "0" else mempty) <> fromString (showFixed False secs6)
  where
    secs6 :: Fixed E6
    secs6 = fromRational (toRational (todSec d))

instance HasSqlValueSyntax OraValueSyntax NominalDiffTime where
    sqlValueSyntax d =
        let dWhole = abs (floor d) :: Int
            hours   = dWhole `div` 3600 :: Int

            d' = dWhole - (hours * 3600)
            minutes = d' `div` 60

            seconds = abs d - fromIntegral ((hours * 3600) + (minutes * 60))

            secondsFixed :: Fixed E12
            secondsFixed = fromRational (toRational seconds)
        in
          OraValueSyntax $
          emit ((if d < 0 then "-" else mempty) <>
                (if hours < 10 then "0" else mempty) <> intDec hours <> ":" <>
                (if minutes < 10 then "0" else mempty) <> intDec minutes <> ":" <>
                (if secondsFixed < 10 then "0" else mempty) <> fromString (showFixed False secondsFixed))

instance HasSqlValueSyntax OraValueSyntax LocalTime where
    sqlValueSyntax d = OraValueSyntax (emit ("'" <> dayBuilder (localDay d) <>
                                             " " <> todBuilder (localTimeOfDay d) <> "'"))

instance HasSqlValueSyntax OraValueSyntax x => HasSqlValueSyntax OraValueSyntax (Maybe x) where
    sqlValueSyntax Nothing = sqlValueSyntax SqlNull
    sqlValueSyntax (Just x) = sqlValueSyntax x

fromSqlConstraintAttributes :: SqlConstraintAttributesBuilder -> OraSyntax
fromSqlConstraintAttributes (SqlConstraintAttributesBuilder timing deferrable) =
  maybe mempty timingBuilder timing <> maybe mempty deferrableBuilder deferrable
  where timingBuilder InitiallyDeferred = emit "INITIALLY DEFERRED"
        timingBuilder InitiallyImmediate = emit "INITIALLY IMMEDIATE"
        deferrableBuilder False = emit "NOT DEFERRABLE"
        deferrableBuilder True = emit "DEFERRABLE"

oraOptPrec :: Maybe Word -> OraSyntax
oraOptPrec Nothing = mempty
oraOptPrec (Just x) = emit "(" <> emit (fromString (show x)) <> emit ")"

oraOptCharSet :: Maybe T.Text -> OraSyntax
oraOptCharSet Nothing = mempty
oraOptCharSet (Just cs) = emit " CHARACTER SET " <> emit (byteString $ TE.encodeUtf8 cs)

oraOptNumericPrec :: Maybe (Word, Maybe Word) -> OraSyntax
oraOptNumericPrec Nothing = mempty
oraOptNumericPrec (Just (prec, Nothing)) = oraOptPrec (Just prec)
oraOptNumericPrec (Just (prec, Just dec)) = emit "(" <> emit (fromString (show prec)) <> emit ", " <> emit (fromString (show dec)) <> emit ")"


-- * Equality checks
#define HAS_ORACLE_EQUALITY_CHECK(ty)                       \
  instance HasSqlEqualityCheck OraExpressionSyntax (ty); \
  instance HasSqlQuantifiedEqualityCheck OraExpressionSyntax (ty);

HAS_ORACLE_EQUALITY_CHECK(Double)
HAS_ORACLE_EQUALITY_CHECK(Float)
HAS_ORACLE_EQUALITY_CHECK(Int)
HAS_ORACLE_EQUALITY_CHECK(Int8)
HAS_ORACLE_EQUALITY_CHECK(Int16)
HAS_ORACLE_EQUALITY_CHECK(Int32)
HAS_ORACLE_EQUALITY_CHECK(Int64)
HAS_ORACLE_EQUALITY_CHECK(Integer)
HAS_ORACLE_EQUALITY_CHECK(Word)
HAS_ORACLE_EQUALITY_CHECK(Word8)
HAS_ORACLE_EQUALITY_CHECK(Word16)
HAS_ORACLE_EQUALITY_CHECK(Word32)
HAS_ORACLE_EQUALITY_CHECK(Word64)
HAS_ORACLE_EQUALITY_CHECK(T.Text)
HAS_ORACLE_EQUALITY_CHECK(TL.Text)
HAS_ORACLE_EQUALITY_CHECK([Char])
HAS_ORACLE_EQUALITY_CHECK(Scientific)
HAS_ORACLE_EQUALITY_CHECK(Day)
HAS_ORACLE_EQUALITY_CHECK(TimeOfDay)
HAS_ORACLE_EQUALITY_CHECK(NominalDiffTime)
HAS_ORACLE_EQUALITY_CHECK(LocalTime)
