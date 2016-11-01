using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Gremlin_beta
{
    class P
    {
        P() { }
        //Predicates
        public static P eq(int i)
        {
            return new P();
        }
        public static P neq(int i)
        {
            return new P();
        }
        public static P lt(int i)
        {
            return new P();
        }
        public static P lte(int i)
        {
            return new P();
        }
        public static P gt(int i)
        {
            return new P();
        }
        public static P gte(int i)
        {
            return new P();
        }
        public static P inside(int low, int high)
        {
            return new P();
        }
        public static P outside(int low, int high)
        {
            return new P();
        }
        public static P between(int low, int high)
        {
            return new P();
        }
        public static P within(int i)
        {
            return new P();
        }

        public static P within(params string[] V)
        {
            return new P();
        }
        public static P without(int i)
        {
            return new P();
        }

        public static P without(params string[] V)
        {
            return new P();
        }
    }
}
