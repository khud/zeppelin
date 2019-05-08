package org.apache.zeppelin.spark;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventClient;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class VariablesViewTest {

    private SparkInterpreter interpreter;
    private DepInterpreter depInterpreter;

    // catch the streaming output in onAppend
    private volatile String output = "";
    // catch the interpreter output in onUpdate
    private InterpreterResultMessageOutput messageOutput;

    private RemoteInterpreterEventClient mockRemoteEventClient;

    @Before
    public void setUp() {
        mockRemoteEventClient = mock(RemoteInterpreterEventClient.class);
    }

    @After
    public void tearDown() throws InterpreterException {
        if (this.interpreter != null) {
            this.interpreter.close();
        }
//        if (this.depInterpreter != null) {
//            this.depInterpreter.close();
//        }
        SparkShims.reset();
    }

    @Test
    public void testSimpleVarsAndCollections() throws InterpreterException {
        interpreter = getInterpreter(false);

        interpreter.internalInterpret("val x = 1", getInterpreterContext());
        VariableView view = interpreter.getVariableView();
        assertNotNull(view);

        JSONObject json = new JSONObject(view.toJson());
        JSONObject x = json.getJSONObject("x");
        assertEquals(2, x.keySet().size());
        assertEquals("1", x.getString("value"));
        assertEquals("Int", x.getString("type"));
        assertEquals(1, json.keySet().size());

        interpreter.internalInterpret("val list = List(1,2,3,4)", getInterpreterContext());
        json = new JSONObject(view.toJson());
        JSONObject list = json.getJSONObject("list");
        assertEquals(3, list.keySet().size());
        assertEquals(4, list.getInt("length"));
        assertEquals("List[Int]", list.getString("type"));
        assertEquals("1,2,3,4", list.getString("value"));
        assertEquals(2, json.keySet().size());

        interpreter.internalInterpret("val map = Map(1 -> 2, 2 -> 3, 3 -> 4)", getInterpreterContext());
        json = new JSONObject(view.toJson());
        JSONObject map = json.getJSONObject("map");
        assertEquals(2, map.keySet().size());
        // FIXME: length should be here too
        // assertEquals(3, map.getInt("length"));
        assertEquals("scala.collection.immutable.Map[Int,Int]", map.getString("type"));
        assertEquals("Map(1 -> 2, 2 -> 3, 3 -> 4)", map.getString("value"));
        assertEquals(3, json.keySet().size());

        interpreter.internalInterpret("1 + 1", getInterpreterContext());
        json = new JSONObject(view.toJson());
        JSONObject res1 = json.getJSONObject("res1");
        assertEquals(2, res1.keySet().size());
        assertEquals("2", res1.getString("value"));
        assertEquals("Int", res1.getString("type"));
        assertEquals(4, json.keySet().size());
    }

    @Test
    public void testObjects() throws InterpreterException {
        interpreter = getInterpreter(false);

        interpreter.internalInterpret("class A(val x: Int)", getInterpreterContext());
        interpreter.internalInterpret("val a = new A(1)", getInterpreterContext());
        VariableView view = interpreter.getVariableView();
        assertNotNull(view);

        JSONObject json = new JSONObject(view.toJson());
        JSONObject a = json.getJSONObject("a");
        assertEquals(2, a.keySet().size());
        assertEquals("iw$A", a.getString("type"));
        JSONObject aObj = a.getJSONObject("value");
        assertEquals(1, aObj.keySet().size());
        JSONObject ax = aObj.getJSONObject("x");
        assertEquals(2, ax.keySet().size());
        assertEquals("1", ax.getString("value"));
        assertEquals("scala.Int", ax.getString("type"));
        assertEquals(1, json.keySet().size());

        String qDef = "class Q {\n" +
                "val a = Array(1,2,3)\n" +
                "val b = List(\"hello\", \"world\")\n" +
                "val c: List[List[String]] = List()\n" +
                "var y = 10\n" +
                "def m(): Int = 10\n" +
                "}";

        interpreter.internalInterpret(qDef, getInterpreterContext());
        interpreter.internalInterpret("val q = new Q()", getInterpreterContext());
        json = new JSONObject(view.toJson());
        JSONObject qObj = json.getJSONObject("q").getJSONObject("value");
        assertEquals(4, qObj.keySet().size());

        assertEquals(3, qObj.getJSONObject("a").get("length"));
        assertEquals("scala.List[scala.List[String]]", qObj.getJSONObject("c").getString("type"));
    }

    public SparkInterpreter getInterpreter(boolean changesOnly) throws InterpreterException {
        Properties properties = new Properties();
        properties.setProperty("spark.master", "local");
        properties.setProperty("spark.app.name", "test");
        properties.setProperty("zeppelin.spark.maxResult", "100");
        properties.setProperty("zeppelin.spark.test", "true");
        properties.setProperty("zeppelin.spark.useNew", "true");
        properties.setProperty("zeppelin.spark.uiWebUrl", "fake_spark_weburl");
        // disable color output for easy testing
        properties.setProperty("zeppelin.spark.scala.color", "false");
        properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

        InterpreterContext context = InterpreterContext.builder()
                .setInterpreterOut(new InterpreterOutput(null))
                .setIntpEventClient(mockRemoteEventClient)
                .setAngularObjectRegistry(new AngularObjectRegistry("spark", null))
                .build();
        InterpreterContext.set(context);

        SparkInterpreter interpreter = new SparkInterpreter(properties);
//        assertTrue(interpreter.getDelegation() instanceof NewSparkInterpreter);
        interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
        interpreter.open();
        return interpreter;
    }

    private InterpreterContext getInterpreterContext() {
        output = "";
        InterpreterContext context = InterpreterContext.builder()
                .setInterpreterOut(new InterpreterOutput(null))
                .setIntpEventClient(mockRemoteEventClient)
                .setAngularObjectRegistry(new AngularObjectRegistry("spark", null))
                .build();
        context.out =
                new InterpreterOutput(

                        new InterpreterOutputListener() {
                            @Override
                            public void onUpdateAll(InterpreterOutput out) {

                            }

                            @Override
                            public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
                                try {
                                    output = out.toInterpreterResultMessage().getData();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }

                            @Override
                            public void onUpdate(int index, InterpreterResultMessageOutput out) {
                                messageOutput = out;
                            }
                        });
        return context;
    }
}
