using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.Gremlin_beta.steps
{
    public class Step
    {
        public virtual void ModifyContext(ref Context context) { }

        internal void AddNewAlias(string alias, ref Context context)
        {
            context._InternalAliasList.Add(alias);
            if (alias[0] == 'N') context._NodeCount++;
            else context._EdgeCount++;
        }

        internal void AddNewPrimaryAlias(string alias, ref Context context)
        {
            context._PrimaryInternalAlias.Add(new WColumnReferenceExpression() { MultiPartIdentifier = CutStringIntoMultiPartIdentifier(alias) });
        }

        internal void ChangePrimaryAlias(string alias, ref Context context)
        {
            context._PrimaryInternalAlias.Clear();
            context._PrimaryInternalAlias.Add(new WColumnReferenceExpression() { MultiPartIdentifier = CutStringIntoMultiPartIdentifier(alias) });
        }

        internal void AddNewPredicates(ref Context sink, WBooleanExpression source)
        {
            if (source != null && sink._AliasPredicates != null)
            {
                sink._AliasPredicates = new WBooleanBinaryExpression()
                {
                    BooleanExpressionType = BooleanBinaryExpressionType.And,
                    FirstExpr = source,
                    SecondExpr = sink._AliasPredicates
                };
            }
        }

        internal WBooleanExpression AddNewOrPredicates(List<WBooleanExpression> ListOfBoolean)
        {
            WBooleanExpression res = null;
            foreach (var x in ListOfBoolean)
            {
                if (x != null && res != null)
                {
                    res = new WBooleanBinaryExpression()
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.Or,
                        FirstExpr = x,
                        SecondExpr = res
                    };
                }
                if (x != null && res == null)
                {
                    res = x;
                }
            }
            return res;
        }

        internal WBooleanExpression AddNewAndPredicates(List<WBooleanExpression> ListOfBoolean)
        {
            WBooleanExpression res = null;
            foreach (var x in ListOfBoolean)
            {
                if (x != null && res != null)
                {
                    res = new WBooleanBinaryExpression()
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.And,
                        FirstExpr = x,
                        SecondExpr = res
                    };
                }
                if (x != null && res == null)
                {
                    res = x;
                }
            }
            return res;
        }
        internal WMultiPartIdentifier CutStringIntoMultiPartIdentifier(string identifier)
        {
            var MultiIdentifierList = new List<Identifier>();
            while (identifier.IndexOf('.') != -1)
            {
                int cutPoint = identifier.IndexOf('.');
                MultiIdentifierList.Add(new Identifier() {
                    Value = identifier.Substring(0, cutPoint)
                });
                identifier = identifier.Substring(cutPoint + 1, identifier.Length - cutPoint - 1);
            }
            MultiIdentifierList.Add(new Identifier() { Value = identifier });
            return new WMultiPartIdentifier() { Identifiers = MultiIdentifierList };
        }
    }
}
