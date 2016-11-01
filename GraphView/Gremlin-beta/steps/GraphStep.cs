using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Gremlin_beta.steps
{
    public class GraphStep: Step
    {
        public GraphStep() {}

        public override void ModifyContext(ref Context pContext)
        {
            string SrcNode = "N_" + pContext._NodeCount.ToString();
            this.AddNewAlias(SrcNode, ref pContext);
            this.ChangePrimaryAlias(SrcNode, ref pContext);
        }
    }
}
