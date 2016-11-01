using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;


namespace GraphView.Gremlin_beta
{
    public class Context
    {
        internal List<WScalarExpression> _PrimaryInternalAlias { get; set; }
        internal List<string> _InternalAliasList { get; set; }
        internal WBooleanExpression _AliasPredicates { get; set; }
        internal List<Tuple<string, string, string>> _Paths { get; set; }
        internal List<string> _Identifiers { get; set; }
        internal Dictionary<string, object> _Properties { get; set; }
        internal Dictionary<string, string> _ExplictAliasToInternalAlias { get; set; }
        internal List<Context> _BranchContexts;
        internal string _BranchNote;
        internal int _NodeCount;
        internal int _EdgeCount;
        internal int _PathCount;
        internal bool _AddEMark;
        internal bool _AddVMark;
        internal bool _RemoveMark;
        internal bool _ChooseMark;
        internal bool _RepeatMark;
        internal bool _HoldMark;
        internal bool _OrderMark;
        internal bool _DoubleAddEMark;
        internal bool _OutputPathMark;
        internal WOrderByClause _ByWhat;
        internal WGroupByClause _Group;
        internal int _limit;

        public Context(Context rhs)
        {
            _BranchNote = rhs._BranchNote;
            _NodeCount = rhs._NodeCount;
            _EdgeCount = rhs._EdgeCount;
            _PathCount = rhs._PathCount;
            _AddEMark = rhs._AddEMark;
            _AddVMark = rhs._AddVMark;
            _RemoveMark = rhs._RemoveMark;
            _ChooseMark = rhs._ChooseMark;
            _HoldMark = rhs._HoldMark;
            _OrderMark = rhs._OrderMark;
            _ByWhat = rhs._ByWhat;
            _DoubleAddEMark = rhs._DoubleAddEMark;
            _OutputPathMark = rhs._OutputPathMark;
            _AliasPredicates = rhs._AliasPredicates;

            _PrimaryInternalAlias = new List<WScalarExpression>();
            foreach (var x in rhs._PrimaryInternalAlias)
                _PrimaryInternalAlias.Add(x);

            foreach (var x in rhs._InternalAliasList)
                _InternalAliasList.Add(x);

            _Paths = new List<Tuple<string, string, string>>();
            foreach (var x in rhs._Paths)
                _Paths.Add(x);

            _Identifiers = new List<string>();
            foreach (var x in rhs._Identifiers)
                _Identifiers.Add(x);

            _ExplictAliasToInternalAlias = new Dictionary<string, string>();
            foreach (var x in rhs._ExplictAliasToInternalAlias)
                _ExplictAliasToInternalAlias.Add(x.Key, x.Value);

            if (rhs._BranchContexts != null)
            {
                _BranchContexts = new List<Context>();
                foreach (var x in rhs._BranchContexts)
                    _BranchContexts.Add(x);
            }
            
        }

        public Context()
        {
            _PrimaryInternalAlias = new List<WScalarExpression>();
            _InternalAliasList = new List<string>();
            _Paths = new List<Tuple<string, string, string>>();
            _ExplictAliasToInternalAlias = new Dictionary<string, string>();
            _Properties = new Dictionary<string, object>();
            _BranchContexts = new List<Context>();
            _Identifiers = new List<string>();
            _ByWhat = new WOrderByClause();

            _NodeCount = 0;
            _EdgeCount = 0;
            _PathCount = 0;
            _limit = -1;
            _AliasPredicates = null;
            _AddEMark = false;
            _AddVMark = false;
            _RemoveMark = false;
            _ChooseMark = false;
            _HoldMark = true;
            _OrderMark = false;
            _DoubleAddEMark = false;
            _OutputPathMark = false;
        }

        public WSqlStatement TransformToSqlTree()
        {
            WSqlStatement SqlTree;

            var SelectStatement = new WSelectStatement();
            var SelectBlock = SelectStatement.QueryExpr as WSelectQueryBlock;

            // Consturct the new From Clause
            var NewFromClause = new WFromClause() { TableReferences = new List<WTableReference>() };
            foreach (var a in _InternalAliasList)
            {
                bool AddToFromFlag = false;
                if (a.Contains("N_"))
                {
                    foreach (var x in _PrimaryInternalAlias)
                    {
                        if (x.ToString().IndexOf(a) != -1) AddToFromFlag = true;
                    }
                    foreach (var x in _Paths)
                    {
                        if ((x.Item1.IndexOf(a) != -1) || (x.Item3.IndexOf(a) != -1))
                            AddToFromFlag = true;
                    }
                    if (!AddToFromFlag) continue;
                    var TR = new WNamedTableReference()
                    {
                        Alias = new Identifier() { Value = a },
                        TableObjectString = "node",
                        TableObjectName = new WSchemaObjectName(new Identifier() { Value = "node" })
                    };
                    NewFromClause.TableReferences.Add(TR);
                }
            }

            // Consturct the new Match Clause
            var NewMatchClause = new WMatchClause() { Paths = new List<WMatchPath>() };
            foreach (var path in _Paths)
            {
                var PathEdges = new List<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>>();
                PathEdges.Add(new Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>
                    (new WSchemaObjectName()
                    {
                        Identifiers = new List<Identifier>()
                        {
                                new Identifier() {Value = path.Item1}
                        }
                    },
                        new WEdgeColumnReferenceExpression()
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier()
                                {
                                    Identifiers = new List<Identifier>() { new Identifier() { Value = "Edge" } }
                                },
                            Alias = path.Item2,
                            MinLength = 1,
                            MaxLength = path.Item2.StartsWith("P_") ? 2 : 1,
                        }));
                var TailNode = new WSchemaObjectName()
                {
                    Identifiers = new List<Identifier>()
                            {
                                new Identifier() {Value = path.Item3}
                            }
                };
                var NewPath = new WMatchPath() { PathEdgeList = PathEdges, Tail = TailNode };
                NewMatchClause.Paths.Add((NewPath));
            }

            // Consturct the new Select Component

            var NewSelectElementClause = new List<WSelectElement>();
            foreach (var alias in _PrimaryInternalAlias)
            {
                NewSelectElementClause.Add(new WSelectScalarExpression() { SelectExpr = alias });
            }

            // Consturct the new Where Clause

            var NewWhereClause = new WWhereClause() { SearchCondition = _AliasPredicates };

            SelectBlock = new WSelectQueryBlock()
            {
                FromClause = NewFromClause,
                SelectElements = NewSelectElementClause,
                WhereClause = NewWhereClause,
                MatchClause = NewMatchClause,
                OrderByClause = _ByWhat,
                OutputPath = _OutputPathMark
            };

            SqlTree = SelectBlock;

            // If needed to add vertex, consturct new InsertNodeStatement
            if (_AddVMark)
            {
                var columnV = new List<WScalarExpression>();
                var columnK = new List<WColumnReferenceExpression>();
                foreach (var property in _Properties)
                {

                    WValueExpression value = null;
                    if (property.Value.GetType() == typeof(double) || property.Value.GetType() == typeof(int))
                        value = new WValueExpression(property.Value.ToString(), false);
                    if (property.Value.GetType() == typeof(string))
                        value = new WValueExpression(property.Value.ToString(), true);
                    columnV.Add(value);
                    var key = new WColumnReferenceExpression()
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier()
                            {
                                Identifiers = new List<Identifier>() { new Identifier() { Value = property.Key } }
                            }
                    };
                    columnK.Add(key);
                }
                var row = new List<WRowValue>() { new WRowValue() { ColumnValues = columnV } };
                var source = new WValuesInsertSource() { RowValues = row };
                var target = new WNamedTableReference() { TableObjectString = "Node" };
                var InsertStatement = new WInsertSpecification()
                {
                    Columns = columnK,
                    InsertSource = source,
                    Target = target
                };
                var InsertNode = new WInsertNodeSpecification(InsertStatement);
                SqlTree = InsertNode;
                return SqlTree;
            }

            // If needed to add edge, consturct new InsertEdgeStatement

            if (_AddEMark)
            {
                var columnV = new List<WScalarExpression>();
                var columnK = new List<WColumnReferenceExpression>();

                foreach (var property in _Properties)
                {
                    WValueExpression value = null;
                    if (property.Value.GetType() == typeof(double) || property.Value.GetType() == typeof(int))
                        value = new WValueExpression(property.Value.ToString(), false);
                    if (property.Value.GetType() == typeof(string))
                        value = new WValueExpression(property.Value.ToString(), true);
                    columnV.Add(value);
                    var key = new WColumnReferenceExpression()
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier()
                            {
                                Identifiers = new List<Identifier>() { new Identifier() { Value = property.Key } }
                            }
                    };
                    SelectBlock.SelectElements.Add(new WSelectScalarExpression() { SelectExpr = value });
                    columnK.Add(key);
                }
                var row = new List<WRowValue>() { new WRowValue() { ColumnValues = columnV } };
                var source = new WValuesInsertSource() { RowValues = row };
                var target = new WNamedTableReference() { TableObjectString = "Edge" };
                var InsertStatement = new WInsertSpecification()
                {
                    Columns = columnK,
                    InsertSource = new WSelectInsertSource() { Select = SelectBlock },
                    Target = target
                };
                var InsertEdge = new WInsertEdgeSpecification(InsertStatement) { SelectInsertSource = new WSelectInsertSource() { Select = SelectBlock } };
                SqlTree = InsertEdge;
                return SqlTree;
            }

            // If needed to remove node/edge, construct new deleteEdge/Node Specification
            if (_RemoveMark)
            {
                if (_PrimaryInternalAlias[0].ToString().IndexOf("N_") != -1)
                {
                    var TargetClause = new WNamedTableReference()
                    {
                        TableObjectName =
                            new WSchemaObjectName()
                            {
                                Identifiers = new List<Identifier>() { new Identifier() { Value = "Node" } }
                            },
                        TableObjectString = "Node",
                        Alias = new Identifier() { Value = _PrimaryInternalAlias.First().ToString() }
                    };
                    var DeleteNodeSp = new WDeleteNodeSpecification()
                    {
                        WhereClause = NewWhereClause,
                        FromClause = NewFromClause,
                        Target = TargetClause
                    };
                    SqlTree = DeleteNodeSp;
                    return SqlTree;
                }
                if (_PrimaryInternalAlias[0].ToString().IndexOf("E_") != -1)
                {
                    var EC = new WEdgeColumnReferenceExpression()
                    {
                        Alias = _PrimaryInternalAlias.First().ToString(),
                        MultiPartIdentifier =
                            new WMultiPartIdentifier()
                            {
                                Identifiers = new List<Identifier>() { new Identifier() { Value = "Edge" } }
                            }
                    };
                    var DeleteEdgeSp = new WDeleteEdgeSpecification(SelectBlock);
                    SqlTree = DeleteEdgeSp;
                    return SqlTree;
                }
            }
            return SqlTree;
        }

    }
}
