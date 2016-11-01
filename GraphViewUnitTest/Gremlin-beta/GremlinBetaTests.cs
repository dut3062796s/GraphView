using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView.Gremlin_beta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Gremlin_beta.Tests
{
    [TestClass()]
    public class GremlinBetaTests
    {
        [TestMethod()]
        public void GetEnumeratorTest()
        {
            GraphTraversal g = new GraphTraversal();
            g.V();
        }
    }
}