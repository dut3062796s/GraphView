using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using Microsoft.CSharp;
using Microsoft.SqlServer.TransactSql.ScriptDom;
// Add DocumentDB references
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;
using System.Collections;
using System.Data;
using System.Dynamic;
using System.Runtime.CompilerServices;

namespace GraphView
{
    /// <summary>
    /// Record is a raw data sturcture flowing from one data operator to another. 
    /// The interpretation of the record is specified in a data operator or a table. 
    /// 
    /// Given a field name, returns the field's value.
    /// Given a field offset, returns the field's value.
    /// </summary>
    public class Record
    {
        public Record()
        { 
        }
        public Record(Record rhs)
        {
            field = new List<string>(rhs.field);
        }
        public Record(int num)
        {
            field = new List<string>();
            for (int i = 0; i < num; i++)
            {
                field.Add("");
            }
        }
        public string RetriveData(List<string> header,string FieldName)
        {
            if (header.IndexOf(FieldName) == -1) return "";
            else if (field.Count <= header.IndexOf(FieldName)) return "";
            else return field[header.IndexOf(FieldName)];
        }
        public string RetriveData(int index)
        {
            return field[index];
        }
        public int RetriveIndex(string value)
        {
            if (field.IndexOf(value) == -1) return -1;
            else return field.IndexOf(value);
        }
        internal List<string> field;
    }
    
    /// <summary>
    /// DocDBOperator is the basic interface of all operator processor function.
    /// It provides three basic interface about the statue of a operator processor function.
    /// And one interface to execute the operator. 
    /// </summary>
    internal interface IGraphViewProcessor
    {
        bool Status();
        void Open();
        void Close();
        Record Next();
    }
    /// <summary>
    /// The most basic class for all operator processor function,
    /// which implements some of the basic interface.
    /// and provides some useful sturcture like buffer on both input and output sides
    /// </summary>
    public abstract class GraphViewOperator : IGraphViewProcessor
    {
        private bool statue;
        public bool Status()
        {
            return statue;
        }
        public void Open()
        {
            statue = true;
        }
        public void Close()
        {
            statue = false;
        }
        public abstract Record Next();

        public List<string> header;
    }

    public class DocDbReader : IDataReader
    {
        private IEnumerable<dynamic> DataSource;
        private int index;

        public DocDbReader(IEnumerable<dynamic> pDataSource)
        {
            index = -1;
            DataSource = pDataSource;
        }
        public bool Read()
        {
            if (index < DataSource.Count())
            {
                index++;
                return true;
            }
            else return false;
        }
        public object this[string FieldName]
        {
            get { return DataSource.ElementAt(index); }
        }
        public object this[int index]
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int Depth { get; set; }
        public bool IsClosed { get; set; }
        public int RecordsAffected { get; set; }
        public int FieldCount { get; set; }
        public void Close() { throw new NotImplementedException(); }
        public void Dispose() { throw new NotImplementedException(); }
        public bool GetBoolean(int x) { throw new NotImplementedException(); }
        public byte GetByte(Int32 x) { throw new NotImplementedException(); }
        public long GetBytes(Int32 x, Int64 y, Byte[] z, Int32 w, Int32 u) { throw new NotImplementedException(); }
        public char GetChar(Int32 x) { throw new NotImplementedException(); }
        public long GetChars(Int32 x, Int64 y, Char[] z, Int32 w, Int32 u) { throw new NotImplementedException(); }
        public IDataReader GetData(Int32 x) { throw new NotImplementedException(); }
        public string GetDataTypeName(Int32 x) { throw new NotImplementedException(); }
        public DateTime GetDateTime(Int32 x) { throw new NotImplementedException(); }
        public decimal GetDecimal(Int32 x) { throw new NotImplementedException(); }
        public double GetDouble(Int32 x) { throw new NotImplementedException(); }
        public Type GetFieldType(Int32 x) { throw new NotImplementedException(); }
        public float GetFloat(Int32 x) { throw new NotImplementedException(); }
        public Guid GetGuid(Int32 x) { throw new NotImplementedException(); }
        public Int16 GetInt16(Int32 x) { throw new NotImplementedException(); }
        public Int32 GetInt32(Int32 x) { throw new NotImplementedException(); }
        public Int64 GetInt64(Int32 x) { throw new NotImplementedException(); }
        public DataTable GetSchemaTable() { throw new NotImplementedException(); }
        public string GetName(Int32 x) { throw new NotImplementedException(); }
        public int GetOrdinal(string x) { throw new NotImplementedException(); }
        public string GetString(Int32 x) { throw new NotImplementedException(); }
        public object GetValue(Int32 x) { throw new NotImplementedException(); }
        public int GetValues(object[] x) { throw new NotImplementedException(); }
        public bool IsDBNull(Int32 x) { throw new NotImplementedException(); }
        public bool NextResult() { throw new NotImplementedException(); }
    }
}

