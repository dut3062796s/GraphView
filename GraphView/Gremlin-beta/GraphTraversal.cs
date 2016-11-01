using GraphView.Gremlin_beta.steps;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Gremlin_beta
{
    public class GraphTraversal: IEnumerable<Record>
    {

        public class GraphTraversalIterator: IEnumerator<Record>
        {
            private GraphViewExecutionOperator _currentOperator;

            internal Record _currentRecord;
            internal List<string> _elements;

            internal GraphTraversalIterator(GraphViewExecutionOperator pCurrentOperator)
            {
                _currentOperator = pCurrentOperator;
                _elements = new List<string>();
            }

            internal GraphTraversalIterator(GraphViewExecutionOperator pCurrentOperator, List<string> pElements)
            {
                _currentOperator = pCurrentOperator;
                _elements = pElements;
            }

            public bool MoveNext()
            {
                if (_currentOperator == null) Reset();
                if (_currentOperator.State())
                {
                    RawRecord rawResultRecord = _currentOperator.Next();
                    _currentRecord = new Record(rawResultRecord, _elements);
                    if (rawResultRecord != null)
                        return true;
                    else
                        return false;
                }
                else
                {
                    return false;
                }
            }

            public void Reset()
            {

            }

            object IEnumerator.Current
            {
                get
                {
                    return _currentRecord;
                }
            }

            public Record Current
            {
                get
                {
                    return _currentRecord;
                }
            }

            public void Dispose()
            {

            }

        }

        internal GraphTraversalIterator _it;
        internal GraphViewExecutionOperator _CurrentOperator;
        internal GraphViewConnection _connection;
        internal List<int> _TokenIndex;
        internal List<string> _elements;
        internal Context _context;
        internal List<Step> _steps;


        public IEnumerator<Record> GetEnumerator()
        {
            foreach (var step in _steps)
            {
                step.ModifyContext(ref _context);
            }

            WSqlStatement SqlTree = _context.TransformToSqlTree();

            _CurrentOperator = SqlTree.Generate(_connection);
            _elements = new List<string>();
            if (_CurrentOperator is OutputOperator)
            {
                foreach (var x in (_CurrentOperator as OutputOperator).SelectedElement)
                {
                    _elements.Add(x);
                }
            }
            return _it;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public GraphTraversal() { }

        public GraphTraversal(GraphTraversal rhs)
        {

        }

        public GraphTraversal(GraphViewConnection pConnection)
        {

        }

        internal void addStep(Step step)
        {
            this._steps.Add(step);
        }

        //Step
 
        //public GraphTraversal addE(Direction direction, string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)

        public GraphTraversal addE(string edgeLabel)
        {
            return this;
        }

        public GraphTraversal addInE(string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)
        {
            return this;
        }

        public GraphTraversal addOutE(string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)
        {
            return this;
        }

        public GraphTraversal addV()
        {
            return this;
        }

        public GraphTraversal addV(params Object[] propertyKeyValues)
        {
            return this;
        }

        public GraphTraversal addV(string vertexLabel)
        {
            return this;
        }

        public GraphTraversal aggregate(string sideEffectKey)
        {
            return this;
        }

        //public GraphTraversal and(Traversal<?, ?> ..andTraversals)
        //public GraphTraversal as(string stepLabel, params string[] stepLabels)
        //public GraphTraversal barrier()
        //public GraphTraversal barrier(Comsumer<org.apache.tinkerpop.gremlin.process.traversal.traverser.util,.TraverserSet<Object>> barrierConsumer)
        //public GraphTraversal both(params string[] edgeLabels)
        //public GraphTraversal bothE(params string[] edgeLabels)
        //public GraphTraversal branch(Function<Traversal<E>, M> function)
        //public GraphTraversal branch(Traversal<?, M> branchTraversal)
        //public GraphTraversal by()
        //public GraphTraversal by(Comparator<E> comparator)
        //public GraphTraversal by(Function<U, Object> function, Comparator comparator)
        //public GraphTraversal by(Function<V, Object> function)
        //public GraphTraversal by(Order order)
        //public GraphTraversal by(string key)
        //public GraphTraversal by(string key, Comparator<V> comparator)
        //public GraphTraversal by(T token)
        //public GraphTraversal by(Traversal<?, ?> traversal)
        //public GraphTraversal by(Traversal<?, ?> traversal, Comparator comparator)
        //public GraphTraversal cap(string sideEffectKey, params string[] sideEffectKeys)
        //public GraphTraversal choose(Function<E, M> choiceFunction)
        //public GraphTraversal choose(Predicate<E> choosePredicate, Traversal<?, E2> trueChoice, Traversal<?, E2> falseChoice)
        //public GraphTraversal choose(Traversal<?, ?>t traversalPredicate, Travaersal<?, E2> trueChoice, Traversal<?, E2> falseChoice)
        //public GraphTraversal choose(Traversal<?, M> choiceTraversal)
        //public GraphTraversal coalesce(Traversal<?, E2> ..coalesceTraversals)
        //public GraphTraversal coin(double probability)
        //public GraphTraversal constant(E2 e)
        //public GraphTraversal count()
        //public GraphTraversal count(Scope scope)
        //public GraphTraversal cyclicPath()
        //public GraphTraversal dedup(Scope scope, params string[] dedupLabels)
        //public GraphTraversal dedup(params string[] dedupLabels)
        //public GraphTraversal drop()
        //public GraphTraversal emit()
        //public GraphTraversal emit(Predicate<Traversal<E>> emitPredicate)
        //public GraphTraversal emit(Traversal<?, ?> emitTraversal)
        //public GraphTraversal filter(Predicate<Traversal<E>> predicate)
        //public GraphTraversal filter(Traversal<?, ?> filterTraversal)
        //public GraphTraversal flatMap(Funtion<Traversal<E>, Iterator<E>> funtion)
        //public GraphTraversal flatMap(Traversal<?, E2> flatMapTraversal)
        //public GraphTraversal fold()
        //public GraphTraversal fold(E2 seed, BiFuntion<E2, E, E2> foldFunction)
        //public GraphTraversal from(string fromStepLabel)
        //public GraphTraversal from(Traversal<E, Vertex> fromVertex)
        //public GraphTraversal group()
        //public GraphTraversal group(string sideEffectKey)
        //public GraphTraversal groupCount()
        //public GraphTraversal groupCount(string sideEffectKey)
        //public GraphTraversal groupV3d0() //Deprecated
        //public GraphTraversal groupV3d0(string sideEffectKey) //Deprecated

        public GraphTraversal has(string propertyKey)
        {
            return this;
        }

        public GraphTraversal has(string propertyKey, Object value)
        {
            return this;
        }

        //public GraphTraversal has(string propertyKey, P<?> predicate)

        public GraphTraversal has(string label, string propertyKey, Object value)
        {
            return this;
        }

        //public GraphTraversal has(string label, string propertyKey, Predicate<?> predicate)
        //public GraphTraversal has(string propertyKey, Traversal<?, ?> propertyTraversal)
        //public GraphTraversal has(T accessor, Object value)
        //public GraphTraversal has(T accessor, Object value, Object ...value)
        //public GraphTraversal has(T accessor, Traversal<?, ?> propertyTraversal)

        public GraphTraversal hasId(string value, params string[] values)
        {
            return this;
        }

        public GraphTraversal hasKey(string value, params string[] values)
        {
            return this;
        }

        public GraphTraversal hasLabel(string value, params string[] values)
        {
            return this;
        }

        public GraphTraversal hasNot(string propertyKey)
        {
            return this;
        }

        public GraphTraversal hasValue(string value, params string[] values)
        {
            return this;
        }

        public GraphTraversal id()
        {
            return this;
        }

        public GraphTraversal In(params string[] edgeLabels) {
            return this;
        }

        public GraphTraversal inE(params string[] edgeLabels)
        {
            return this;
        }

        //public GraphTraversal inject()

        public GraphTraversal inV()
        {
            return this;
        }

        public GraphTraversal Is(string value)
        {
            return this;
        }

        //public GraphTraversal Is(P<E> predicate)

        public GraphTraversal iterate()
        {
            return this;
        }

        public GraphTraversal key()
        {
            return this;
        }

        public GraphTraversal label()
        {
            return this;
        }

        public GraphTraversal limit(long limit)
        {
            return this;
        }

        public GraphTraversal limit(Scope scope, long limit)
        {
            return this;
        }

        //public GraphTraversal local(Traversal<?, E2> localTraversal)
        //public GraphTraversal loops()
        //public GraphTraversal map(Function<Traversal<?, E2>> function)
        //public GraphTraversal map(Traversal<?, E2> mapTraversal)
        //public GraphTraversal mapKeys() //Deprecated
        //public GraphTraversal mapvalues() //Deprecated
        //public GraphTraversal match(Traversal<?, ?>..matchTraversals)

        public GraphTraversal max()
        {
            return this;
        }

        public GraphTraversal max(Scope scope)
        {
            return this;
        }

        public GraphTraversal mean()
        {
            return this;
        }

        public GraphTraversal mean(Scope scope)
        {
            return this;
        }

        public GraphTraversal min()
        {
            return this;
        }

        public GraphTraversal min(Scope scope)
        {
            return this;
        }

        //public GraphTraversal not(Traversal<?, ?> notTraversal)
        //public GraphTraversal option(M pickToken, Traversal<E, E2> traversalOption)
        //public GraphTraversal option(Traversal<E, E2 tarversalOption>
        //public GraphTraversal optional(Traversal<E, E2> traversalOption)
        //public GraphTraversal or(Traversal<?, ?> ...orTraversals)

        public GraphTraversal order()
        {
            return this;
        }

        public GraphTraversal order(Scope scope)
        {
            return this;
        }

        public GraphTraversal otherV()
        {
            return this;
        }

        public GraphTraversal Out(params string[] edgeLabels)
        {
            return this;
        }

        public GraphTraversal outE(params string[] edgeLabels)
        {
            return this;
        }

        public GraphTraversal outV()
        {
            return this;
        }

        //public GraphTraversal pageRank()
        //public GraphTraversal pageRank(double alpha)
        //public GraphTraversal path()
        //public GraphTraversal peerPressure()
        //public GraphTraversal profile()
        //public GraphTraversal profile(string sideEffectKey)
        //public GraphTraversal program(VertexProgram<?> vertexProgram)
        //public GraphTraversal project(string projectKey, params string[] otherProjectKeys)

        public GraphTraversal properties(params string[] propertyKeys)
        {
            return this;
        }

        public GraphTraversal property(string key, string value, params string[] keyValues)
        {
            return this;
        }

        //public GraphTraversal property(VertexProperty.Cardinality cardinality, string key, string value, params string[] keyValues)

        public GraphTraversal propertyMap(params string[] propertyKeys)
        {
            return this;
        }

        public GraphTraversal range(long low, long high)
        {
            return this;
        }

        //public GraphTraversal repeat(Traversal<?, E> repeatTraversal)

        //public GraphTraversal sack() //Deprecated
        //public GraphTraversal sack(BiFunction<V, U, V>) sackOperator) //Deprecated
        //public GraphTraversal sack(BiFunction<V, U, V>) sackOperator, string, elementPropertyKey) //Deprecated

        public GraphTraversal sample(int amountToSample)
        {
            return this;
        }

        public GraphTraversal sample(Scope scope, int amountToSample)
        {
            return this;
        }

        //public GraphTraversal select(Column column)
        //public GraphTraversal select(Pop pop, string selectKey)
        //public GraphTraversal select(Pop pop, string selectKey1, string selectKey2, params string[] otherSelectKeys)
        //public GraphTraversal select(string selectKey)
        //public GraphTraversal select(string selectKey1, string selectKey2, params string[] otherSelectKeys)
        //public GraphTraversal sideEffect(Consumer<Traverser<E>> consumer)
        //public GraphTraversal sideEffect(Traversal<?, ?> sideEffectTraversal)
        //public GraphTraversal simplePath()
        //public GraphTraversal store(string sideEffectKey)
        //public GraphTraversal subgraph(string sideEffectKey)

        public GraphTraversal sum()
        {
            return this;
        }

        public GraphTraversal sum(Scope scope)
        {
            return this;
        }

        public GraphTraversal tail()
        {
            return this;
        }

        public GraphTraversal tail(long limit)
        {
            return this;
        }

        public GraphTraversal tail(Scope scope)
        {
            return this;

        }

        public GraphTraversal tail(Scope scope, long limit)
        {
            return this;
        }

        public GraphTraversal timeLimit(long timeLimit)
        {
            return this;
        }

        public GraphTraversal times(int maxLoops)
        {
            return this;
        }

        //public GraphTraversal to(Direction direction, params string[] edgeLabels)

        public GraphTraversal to(string toStepLabel)
        {
            return this;
        }

        //public GraphTraversal to(Traversal<E, Vertex> toVertex)
        //public GraphTraversal toE(Direction direction, params string[] edgeLabels)
        //public GraphTraversal toV(Direction direction)
        //public GraphTraversal tree()
        //public GraphTraversal tree(string sideEffectKey)

        public GraphTraversal unfold()
        {
            return this;
        }

        //public GraphTraversal union(params Traversal<?, E2>[] unionTraversals)
        //public GraphTraversal until(Predicate<Traverser<E>> untilPredicate)
        //public GraphTraversal unitl(Traversal<?, ?> untilTraversal)

        public GraphTraversal V(params Object[] vertexIdsOrElements)
        {
            this.addStep(new GraphStep());
            return this;
        }

        public GraphTraversal value()
        {
            return this;
        }

        public GraphTraversal valueMap(Boolean includeTokens, params string[] propertyKeys)
        {
            return this;
        }

        public GraphTraversal valueMap(params string[] propertyKeys)
        {
            return this;
        }

        public GraphTraversal values(params string[] propertyKeys)
        {
            return this;
        }

        //public GraphTraversal where(P<string> predicate)
        //public GraphTraversal where(string startKey, P<string> predicate)
        //public GraphTraversal where(Traversal<?, ?> whereTraversal)
    }
}
