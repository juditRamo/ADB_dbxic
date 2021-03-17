/* Generated By:JavaCC: Do not edit this line. XicQLParser.java */
package org.adbs.dbxic.engine.parser;

import org.adbs.dbxic.catalog.Attribute;
import org.adbs.dbxic.catalog.Catalog;
import org.adbs.dbxic.catalog.Table;
import org.adbs.dbxic.engine.algebra.*;
import org.adbs.dbxic.engine.core.*;
import org.adbs.dbxic.utils.Logics;
import org.adbs.dbxic.utils.Pair;

import java.util.ArrayList;
import java.util.List;



public class XicQLParser implements XicQLParserConstants {

       private Catalog catalog;

        public void setCatalog(Catalog cat) {
               this.catalog = cat;
        }

        public static void main(String args[]) {
                System.out.println("Reading from standard input...");
                XicQLParser t = new XicQLParser(System.in);
                try {
                        t.Start();
                        System.out.println("Good bye!");
                } catch (Exception e) {
                        System.out.println("Error:" + e.getMessage());
                        e.printStackTrace();
                }
        }

/*
TOKEN:
{
	< EMPTY: "" >
}
*/

/****************************************************
 ** The SQL grammar starts from this point forward **
 ****************************************************/
  static final public Statement Start() throws ParseException {
        Object o = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case SELECT:
      o = Query();
                                // safe to ignore the warning since we are
                                // casting to the return type
                                @SuppressWarnings("unchecked")
                                List<AlgebraicOperator> ops =
                                        (List<AlgebraicOperator>) o;
                                {if (true) return new Query(ops);}
      break;
    case CREATE:
      o = Create();
                                {if (true) return new TableCreation((Table) o);}
      break;
    case DROP:
      o = Drop();
                                {if (true) return new TableDeletion((String) o);}
      break;
    case INSERT:
      o = Insert();
                                // safe to ignore the warning since we are
                                // casting to the return type
                                @SuppressWarnings("unchecked")
                                Pair<String, List<Comparable>> pair =
                                     (Pair<String, List<Comparable>>) o;
                                {if (true) return new TupleInsertion(pair.first,
                                                          pair.second);}
      break;
    case CATALOG:
      Catalog();
                                {if (true) return new ShowCatalog();}
      break;
    case COMMANDLIST:
      CommandList();
                    {if (true) return new ShowCommandList();}
      break;
    case DESCRIBE:
      o = Describe();
                                {if (true) return new TableDescription((String) o);}
      break;
    default:
      jj_la1[0] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> Query() throws ParseException {
        List<AlgebraicOperator> algebra = new ArrayList<AlgebraicOperator>();
        List<AlgebraicOperator> where = null;
        Projection p = null;
        Sort s = null;
        Group g = null;
    p = SelectClause();
                        algebra.add(p);
    FromClause();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case WHERE:
      where = WhereClause();
                                algebra.addAll(where);
      break;
    default:
      jj_la1[1] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ORDER:
      s = SortClause();
                                algebra.add(s);
      break;
    default:
      jj_la1[2] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case GROUP:
      g = GroupClause();
                                algebra.add(g);
      break;
    default:
      jj_la1[3] = jj_gen;
      ;
    }
                        {if (true) return algebra;}
    throw new Error("Missing return statement in function");
  }

  static final public Projection SelectClause() throws ParseException {
        List<Variable> projections = new ArrayList<Variable>();
    jj_consume_token(SELECT);
    projections = AttributeList();
                        //System.out.println(projections);
                        Projection p = new Projection(projections);
                        {if (true) return p;}
    throw new Error("Missing return statement in function");
  }

  static final public void FromClause() throws ParseException {
    jj_consume_token(FROM);
    TableList();
  }

  static final public List<AlgebraicOperator> WhereClause() throws ParseException {
        List<AlgebraicOperator> v = null;
    jj_consume_token(WHERE);
    v = BooleanExpression();
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public Sort SortClause() throws ParseException {
        List<Variable> attributes = new ArrayList<Variable>();
    jj_consume_token(ORDER);
    jj_consume_token(BY);
    attributes = AttributeList();
                        Sort s = new Sort(attributes);
                        {if (true) return s;}
    throw new Error("Missing return statement in function");
  }

  static final public Group GroupClause() throws ParseException {
        List<Variable> attributes = new ArrayList<Variable>();
    jj_consume_token(GROUP);
    jj_consume_token(BY);
    attributes = AttributeList();
                        Group g = new Group(attributes);
                        {if (true) return g;}
    throw new Error("Missing return statement in function");
  }

  static final public List<Variable> AttributeList() throws ParseException {
        List<Variable> v = new ArrayList<Variable>();
        Variable var = null;
    var = Attribute();
                        v.add(var);
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[4] = jj_gen;
        break label_1;
      }
      jj_consume_token(COMMA);
      var = Attribute();
                        v.add(var);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public void TableList() throws ParseException {
    Table();
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[5] = jj_gen;
        break label_2;
      }
      jj_consume_token(COMMA);
      Table();
    }
  }

  static final public String Table() throws ParseException {
        String x = null;
    if (jj_2_1(2147483647)) {
      AliasedTable();
                                {if (true) throw new ParseException("Table aliases not "
                                                         + "yet supported.");}
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ID:
        x = Identifier();
                                {if (true) return x;}
        break;
      default:
        jj_la1[6] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
    throw new Error("Missing return statement in function");
  }

  static final public Variable Attribute() throws ParseException {
        Variable var = null;
    if (jj_2_2(2147483647)) {
      var = QualifiedAttribute();
                                {if (true) return var;}
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ID:
        Identifier();
                                {if (true) throw new ParseException("Unqualified "
                                                         + "attributes not "
                                                         + "yet supported.");}
        break;
      default:
        jj_la1[7] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> BooleanExpression() throws ParseException {
        List<AlgebraicOperator> v = new ArrayList<AlgebraicOperator>();
    v = DisjunctiveExpression();
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> DisjunctiveExpression() throws ParseException {
        List<AlgebraicOperator> v = new ArrayList<AlgebraicOperator>();
    v = ConjunctiveExpression();
    label_3:
    while (true) {
      if (jj_2_3(2147483647)) {
        ;
      } else {
        break label_3;
      }
      DisjunctionOperator();
                                        {if (true) throw new ParseException("Disjunction "
                                                                 + "not yet "
                                                                 + "supported");}
      ConjunctiveExpression();
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> ConjunctiveExpression() throws ParseException {
        List<AlgebraicOperator> algebra = new ArrayList<AlgebraicOperator>();
        AlgebraicOperator op = null;
    op = UnaryExpression();
                        algebra.add(op);
    label_4:
    while (true) {
      if (jj_2_4(2147483647)) {
        ;
      } else {
        break label_4;
      }
      ConjunctionOperator();
      op = UnaryExpression();
                                        algebra.add(op);
    }
                        {if (true) return algebra;}
    throw new Error("Missing return statement in function");
  }

  static final public AlgebraicOperator UnaryExpression() throws ParseException {
        AlgebraicOperator op = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case NOT:
      NegationOperator();
      BooleanExpression();
                                {if (true) throw new ParseException("Negation not yet "
                                                         + "supported.");}
      break;
    case OPENPAR:
      jj_consume_token(OPENPAR);
      BooleanExpression();
      jj_consume_token(CLOSEPAR);
                                {if (true) throw new ParseException("Nested expressions "
                                                         + "not yet "
                                                         + "supported.");}
      break;
    case ID:
      op = RelationalExpression();
                                {if (true) return op;}
      break;
    default:
      jj_la1[8] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public AlgebraicOperator RelationalExpression() throws ParseException {
        Variable leftVar = null;
        Variable rightVar = null;
        String val = null;
        Logics.CompareRelation qual = Logics.CompareRelation.EQUALS;
        AlgebraicOperator op = null;
    leftVar = Attribute();
    qual = PropositionOperator();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ID:
      rightVar = Attribute();
                                Proposition propvar = new Proposition<Variable>(qual, leftVar, rightVar);
                                op = new Join(propvar);
      break;
    case INTEGER_LITERAL:
    case FLOATING_POINT_LITERAL:
    case STRING_LITERAL:
      val = Literal();
                                Proposition propval = new Proposition<String>(qual, leftVar, val);
                                op = new Selection(propval);
      break;
    default:
      jj_la1[9] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
                        {if (true) return op;}
    throw new Error("Missing return statement in function");
  }

  static final public void DisjunctionOperator() throws ParseException {
    jj_consume_token(OR);
  }

  static final public void ConjunctionOperator() throws ParseException {
    jj_consume_token(AND);
  }

  static final public void NegationOperator() throws ParseException {
    jj_consume_token(NOT);
  }

  static final public Logics.CompareRelation PropositionOperator() throws ParseException {
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case LESS:
      jj_consume_token(LESS);
        {if (true) return Logics.CompareRelation.LESS;}
      break;
    case LESSEQUAL:
      jj_consume_token(LESSEQUAL);
                                {if (true) return Logics.CompareRelation.LESS_EQUALS;}
      break;
    case GREATER:
      jj_consume_token(GREATER);
        {if (true) return Logics.CompareRelation.GREATER;}
      break;
    case GREATEREQUAL:
      jj_consume_token(GREATEREQUAL);
        {if (true) return Logics.CompareRelation.GREATER_EQUALS;}
      break;
    case EQUAL:
      jj_consume_token(EQUAL);
                                {if (true) return Logics.CompareRelation.EQUALS;}
      break;
    case NOTEQUAL:
      jj_consume_token(NOTEQUAL);
                                {if (true) return Logics.CompareRelation.NOT_EQUALS;}
      break;
    case NOTEQUAL2:
      jj_consume_token(NOTEQUAL2);
                                {if (true) return Logics.CompareRelation.NOT_EQUALS;}
      break;
    default:
      jj_la1[10] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public void AliasedTable() throws ParseException {
    jj_consume_token(ID);
    jj_consume_token(ID);
  }

  static final public Variable QualifiedAttribute() throws ParseException {
        Token table = null;
        Token attr = null;
    table = jj_consume_token(ID);
    jj_consume_token(DOT);
    attr = jj_consume_token(ID);
                        {if (true) return new Variable(table.image, attr.image);}
    throw new Error("Missing return statement in function");
  }

  static final public String Identifier() throws ParseException {
        Token x = null;
    x = jj_consume_token(ID);
                        {if (true) return x.image;}
    throw new Error("Missing return statement in function");
  }

  static final public String Literal() throws ParseException {
        Token x = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case STRING_LITERAL:
      x = jj_consume_token(STRING_LITERAL);
                                String s = x.image;
                                s = s.substring(1, s.length()-1);
                                {if (true) return s;}
      break;
    case INTEGER_LITERAL:
      x = jj_consume_token(INTEGER_LITERAL);
                                        {if (true) return x.image;}
      break;
    case FLOATING_POINT_LITERAL:
      x = jj_consume_token(FLOATING_POINT_LITERAL);
                                               {if (true) return x.image;}
      break;
    default:
      jj_la1[11] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public Table Create() throws ParseException {
        List<Attribute> v = null;
        String table;
    jj_consume_token(CREATE);
    jj_consume_token(TABLE);
    table = Identifier();
    jj_consume_token(OPENPAR);
    v = AttributeDeclarationList(table);
    jj_consume_token(CLOSEPAR);
                                {if (true) return new Table(table, v);}
    throw new Error("Missing return statement in function");
  }

  static final public List<Attribute> AttributeDeclarationList(String table) throws ParseException {
        List<Attribute> v = new ArrayList<Attribute>();
        Attribute attr = null;
    attr = AttributeDeclaration(table);
                        v.add(attr);
    label_5:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[12] = jj_gen;
        break label_5;
      }
      jj_consume_token(COMMA);
      attr = AttributeDeclaration(table);
                                                v.add(attr);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public Attribute AttributeDeclaration(String table) throws ParseException {
        String name = null;
        Class<? extends Comparable> type = null;
    name = Identifier();
    type = Type();
                        {if (true) return new Attribute(name, table, type);}
    throw new Error("Missing return statement in function");
  }

  static final public Class<? extends Comparable> Type() throws ParseException {
        Token token = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case INTEGER:
      jj_consume_token(INTEGER);
                                {if (true) return Integer.class;}
      break;
    case LONG:
      jj_consume_token(LONG);
                                {if (true) return Long.class;}
      break;
    case CHAR:
      jj_consume_token(CHAR);
                                {if (true) return Character.class;}
      break;
    case BYTE:
      jj_consume_token(BYTE);
                                {if (true) return Byte.class;}
      break;
    case SHORT:
      jj_consume_token(SHORT);
                                {if (true) return Short.class;}
      break;
    case DOUBLE:
      jj_consume_token(DOUBLE);
                                {if (true) return Double.class;}
      break;
    case FLOAT:
      jj_consume_token(FLOAT);
                                {if (true) return Float.class;}
      break;
    case STRING:
      jj_consume_token(STRING);
                                {if (true) return String.class;}
      break;
    default:
      jj_la1[13] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public Pair<String, List<Comparable>> Insert() throws ParseException {
        String table = null;
        List<Comparable> v = new ArrayList<Comparable>();
    jj_consume_token(INSERT);
    jj_consume_token(INTO);
    table = Identifier();
    jj_consume_token(VALUES);
    jj_consume_token(OPENPAR);
    v = ValueList();
    jj_consume_token(CLOSEPAR);
                        {if (true) return new Pair<String, List<Comparable>>(table, v);}
    throw new Error("Missing return statement in function");
  }

  static final public List<Comparable> ValueList() throws ParseException {
        List<Comparable> v = new ArrayList<Comparable>();
        String l = null;
    l = Literal();
                        v.add(l);
    label_6:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[14] = jj_gen;
        break label_6;
      }
      jj_consume_token(COMMA);
      l = Literal();
                                v.add(l);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public String Drop() throws ParseException {
        String id = null;
    jj_consume_token(DROP);
    jj_consume_token(TABLE);
    id = Identifier();
                        {if (true) return id;}
    throw new Error("Missing return statement in function");
  }

  static final public void Catalog() throws ParseException {
    jj_consume_token(CATALOG);
  }

  static final public void CommandList() throws ParseException {
    jj_consume_token(COMMANDLIST);
  }

  static final public String Describe() throws ParseException {
        String s = null;
    jj_consume_token(DESCRIBE);
    jj_consume_token(TABLE);
    s = Identifier();
                        {if (true) return s;}
    throw new Error("Missing return statement in function");
  }

  static private boolean jj_2_1(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_1(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(0, xla); }
  }

  static private boolean jj_2_2(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_2(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(1, xla); }
  }

  static private boolean jj_2_3(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_3(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(2, xla); }
  }

  static private boolean jj_2_4(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_4(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(3, xla); }
  }

  static private boolean jj_3_1() {
    if (jj_3R_7()) return true;
    return false;
  }

  static private boolean jj_3_4() {
    if (jj_scan_token(12)) return true;
    return false;
  }

  static private boolean jj_3_2() {
    if (jj_3R_8()) return true;
    return false;
  }

  static private boolean jj_3R_7() {
    if (jj_scan_token(ID)) return true;
    if (jj_scan_token(ID)) return true;
    return false;
  }

  static private boolean jj_3R_8() {
    if (jj_scan_token(ID)) return true;
    if (jj_scan_token(DOT)) return true;
    if (jj_scan_token(ID)) return true;
    return false;
  }

  static private boolean jj_3_3() {
    if (jj_scan_token(13)) return true;
    return false;
  }

  static private boolean jj_initialized_once = false;
  /** Generated Token Manager. */
  static public XicQLParserTokenManager token_source;
  static SimpleCharStream jj_input_stream;
  /** Current token. */
  static public Token token;
  /** Next token. */
  static public Token jj_nt;
  static private int jj_ntk;
  static private Token jj_scanpos, jj_lastpos;
  static private int jj_la;
  /** Whether we are looking ahead. */
  static private boolean jj_lookingAhead = false;
  static private boolean jj_semLA;
  static private int jj_gen;
  static final private int[] jj_la1 = new int[15];
  static private int[] jj_la1_0;
  static private int[] jj_la1_1;
  static private int[] jj_la1_2;
  static {
      jj_la1_init_0();
      jj_la1_init_1();
      jj_la1_init_2();
   }
   private static void jj_la1_init_0() {
      jj_la1_0 = new int[] {0x8000,0x20000,0x40000,0x80000,0x0,0x0,0x0,0x0,0x4000,0x580,0x0,0x580,0x0,0x0,0x0,};
   }
   private static void jj_la1_init_1() {
      jj_la1_1 = new int[] {0x1e3,0x0,0x0,0x0,0x2000,0x2000,0x0,0x0,0x200000,0x0,0x1fc000,0x0,0x2000,0xe0000000,0x2000,};
   }
   private static void jj_la1_init_2() {
      jj_la1_2 = new int[] {0x0,0x0,0x0,0x0,0x0,0x0,0x20,0x20,0x20,0x20,0x0,0x0,0x0,0x1f,0x0,};
   }
  static final private JJCalls[] jj_2_rtns = new JJCalls[4];
  static private boolean jj_rescan = false;
  static private int jj_gc = 0;

  /** Constructor with InputStream. */
  public XicQLParser(java.io.InputStream stream) {
     this(stream, null);
  }
  /** Constructor with InputStream and supplied encoding */
  public XicQLParser(java.io.InputStream stream, String encoding) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    try { jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source = new XicQLParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 15; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Reinitialise. */
  static public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  /** Reinitialise. */
  static public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 15; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Constructor. */
  public XicQLParser(java.io.Reader stream) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser. ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    jj_input_stream = new SimpleCharStream(stream, 1, 1);
    token_source = new XicQLParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 15; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Reinitialise. */
  static public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 15; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Constructor with generated Token Manager. */
  public XicQLParser(XicQLParserTokenManager tm) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser. ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 15; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Reinitialise. */
  public void ReInit(XicQLParserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 15; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  static private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      if (++jj_gc > 100) {
        jj_gc = 0;
        for (int i = 0; i < jj_2_rtns.length; i++) {
          JJCalls c = jj_2_rtns[i];
          while (c != null) {
            if (c.gen < jj_gen) c.first = null;
            c = c.next;
          }
        }
      }
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }

  static private final class LookaheadSuccess extends java.lang.Error { }
  static final private LookaheadSuccess jj_ls = new LookaheadSuccess();
  static private boolean jj_scan_token(int kind) {
    if (jj_scanpos == jj_lastpos) {
      jj_la--;
      if (jj_scanpos.next == null) {
        jj_lastpos = jj_scanpos = jj_scanpos.next = token_source.getNextToken();
      } else {
        jj_lastpos = jj_scanpos = jj_scanpos.next;
      }
    } else {
      jj_scanpos = jj_scanpos.next;
    }
    if (jj_rescan) {
      int i = 0; Token tok = token;
      while (tok != null && tok != jj_scanpos) { i++; tok = tok.next; }
      if (tok != null) jj_add_error_token(kind, i);
    }
    if (jj_scanpos.kind != kind) return true;
    if (jj_la == 0 && jj_scanpos == jj_lastpos) throw jj_ls;
    return false;
  }


/** Get the next Token. */
  static final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

/** Get the specific Token. */
  static final public Token getToken(int index) {
    Token t = jj_lookingAhead ? jj_scanpos : token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  static private int jj_ntk() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  static private java.util.List jj_expentries = new java.util.ArrayList();
  static private int[] jj_expentry;
  static private int jj_kind = -1;
  static private int[] jj_lasttokens = new int[100];
  static private int jj_endpos;

  static private void jj_add_error_token(int kind, int pos) {
    if (pos >= 100) return;
    if (pos == jj_endpos + 1) {
      jj_lasttokens[jj_endpos++] = kind;
    } else if (jj_endpos != 0) {
      jj_expentry = new int[jj_endpos];
      for (int i = 0; i < jj_endpos; i++) {
        jj_expentry[i] = jj_lasttokens[i];
      }
      boolean exists = false;
      for (java.util.Iterator it = jj_expentries.iterator(); it.hasNext();) {
        int[] oldentry = (int[])(it.next());
        if (oldentry.length == jj_expentry.length) {
          exists = true;
          for (int i = 0; i < jj_expentry.length; i++) {
            if (oldentry[i] != jj_expentry[i]) {
              exists = false;
              break;
            }
          }
          if (exists) break;
        }
      }
      if (!exists) jj_expentries.add(jj_expentry);
      if (pos != 0) jj_lasttokens[(jj_endpos = pos) - 1] = kind;
    }
  }

  /** Generate ParseException. */
  static public ParseException generateParseException() {
    jj_expentries.clear();
    boolean[] la1tokens = new boolean[72];
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (int i = 0; i < 15; i++) {
      if (jj_la1[i] == jj_gen) {
        for (int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
          if ((jj_la1_1[i] & (1<<j)) != 0) {
            la1tokens[32+j] = true;
          }
          if ((jj_la1_2[i] & (1<<j)) != 0) {
            la1tokens[64+j] = true;
          }
        }
      }
    }
    for (int i = 0; i < 72; i++) {
      if (la1tokens[i]) {
        jj_expentry = new int[1];
        jj_expentry[0] = i;
        jj_expentries.add(jj_expentry);
      }
    }
    jj_endpos = 0;
    jj_rescan_token();
    jj_add_error_token(0, 0);
    int[][] exptokseq = new int[jj_expentries.size()][];
    for (int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = (int[])jj_expentries.get(i);
    }
    return new ParseException(token, exptokseq, tokenImage);
  }

  /** Enable tracing. */
  static final public void enable_tracing() {
  }

  /** Disable tracing. */
  static final public void disable_tracing() {
  }

  static private void jj_rescan_token() {
    jj_rescan = true;
    for (int i = 0; i < 4; i++) {
    try {
      JJCalls p = jj_2_rtns[i];
      do {
        if (p.gen > jj_gen) {
          jj_la = p.arg; jj_lastpos = jj_scanpos = p.first;
          switch (i) {
            case 0: jj_3_1(); break;
            case 1: jj_3_2(); break;
            case 2: jj_3_3(); break;
            case 3: jj_3_4(); break;
          }
        }
        p = p.next;
      } while (p != null);
      } catch(LookaheadSuccess ls) { }
    }
    jj_rescan = false;
  }

  static private void jj_save(int index, int xla) {
    JJCalls p = jj_2_rtns[index];
    while (p.gen > jj_gen) {
      if (p.next == null) { p = p.next = new JJCalls(); break; }
      p = p.next;
    }
    p.gen = jj_gen + xla - jj_la; p.first = token; p.arg = xla;
  }

  static final class JJCalls {
    int gen;
    Token first;
    int arg;
    JJCalls next;
  }

}
