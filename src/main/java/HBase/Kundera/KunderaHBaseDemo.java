package HBase.Kundera;

import org.junit.*;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.HashMap;
import java.util.Map;

public class KunderaHBaseDemo {
    public static void main(String[] args) {
        TestObject user = new TestObject();
        user.setUserId("0001");
        user.setFirstName("John");
        user.setLastName("Smith");

        //enable CQL3
        Map<String, String> props = new HashMap<>();
        //props.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("hbase_pu", props);
        EntityManager em = emf.createEntityManager();

        em.persist(user);
        em.close();
        emf.close();
    }

    /** The Constant PU. */
    private static final String PU = "hbase_pu";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up before class.
     *
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
    }

    /**
     * Sets the up.
     *
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * Test crud operations.
     *
     * @throws Exception
     *             the exception
     */
    @Test
    public void testCRUDOperations() throws Exception
    {
        testInsert();
        //testMerge();
        //testRemove();
    }

    /**
     * Test insert.
     *
     * @throws Exception
     *             the exception
     */
    private void testInsert() throws Exception
    {
        TestObject user = new TestObject();
        user.setUserId("0001");
        user.setFirstName("John");
        user.setLastName("Smith");

        TestObject person = em.find(TestObject.class, "0001");
        Assert.assertNotNull(person);
        Assert.assertEquals("0001", user.getUserId());
        Assert.assertEquals("John", user.getFirstName());

    }

    /**
     * Test merge.
     */
    private void testMerge()
    {
        TestObject user = em.find(TestObject.class, "0001");
        user.setLastName("Deep");
        em.merge(user);

        TestObject user1 = em.find(TestObject.class, "0001");
        Assert.assertEquals("Deep", user1.getLastName());
    }

    /**
     * Test remove.
     */
    private void testRemove()
    {
        TestObject user = em.find(TestObject.class, "0001");
        em.remove(user);

        TestObject user1 = em.find(TestObject.class, "0001");
        Assert.assertNull(user1);
    }

    /**
     * Tear down.
     *
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
    }

    /**
     * Tear down after class.
     *
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (emf != null)
        {
            emf.close();
            emf = null;
        }
    }
}
