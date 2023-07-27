import java.sql.*;
import java.util.*;
import java.lang.StringBuilder;

/**
 * Class that connects to a Database and estimates the size of joining two relations
 * @author Dan Yee
 */
public class Database
{
	private enum JoinCase {
		NOJOIN, CASE1, CASE2, CASE3, CASE4
	}
	// jdbc:postgresql://localhost/DB_NAME
	private final String URL = "jdbc:postgresql://localhost/university_db";		// REPLACE "university_db" with name of the database
	private final String USER = "postgres"; 									// REPLACE value with username (default: postgres)
	private final String PASSWORD = "REPLACE_ME"; 								// REPLACE value with the master password to your pgAdmin
    private Connection db;
    private DatabaseMetaData metaData;
    private JoinCase joinCase;
    
    private String table1;
    private String table2;
    
    public Database(String table1, String table2) throws Exception
    {
    	this.db = DriverManager.getConnection(URL, USER, PASSWORD);
    	this.metaData = this.db.getMetaData();
    	if (!this.isValidTableNames(table1.toLowerCase(), table2.toLowerCase()))
    		throw new Exception("One or more of the provided table names do not exist in this database.");
    	this.table1 = table1.toLowerCase();
    	this.table2 = table2.toLowerCase();
    	this.joinCase = JoinCase.NOJOIN;
    }
    
    /**
     * Uses DatabaseMetaData to get the names of all the tables in the database as a HashSet
     * @return a HashSet containing the names of all the tables in the database
     * @throws SQLException
     */
    private Set<String> getTableNames() throws SQLException
    {
    	Set<String> tables = new HashSet<String>();
    	ResultSet tableNames = this.metaData.getTables(null, null, "%", new String[]{"TABLE"});
    	while (tableNames.next())
    		tables.add(tableNames.getString("TABLE_NAME"));
    	tableNames.close();
    	return tables;
    }
    
    /**
     * Verifies that the provided table names exist in the database
     * @param table1 name of the first table
     * @param table2 name of the second table
     * @return true if both table names exist in the database; false otherwise
     * @throws SQLException
     */
    private boolean isValidTableNames(String table1, String table2) throws SQLException
    {
    	Set<String> tables = this.getTableNames();
    	return tables.contains(table1) && tables.contains(table2);
    }
    
    /**
     * Gets all the attributes for a relation
     * @param table the name of the relation
     * @return a Set containing all the attributes (columns) of the relation
     * @throws SQLException
     */
    private Set<String> getColumnNames(String table) throws SQLException
    {
    	Set<String> columns = new HashSet<String>();
    	ResultSet columnNames = this.metaData.getColumns(null, null, table, null);
    	while (columnNames.next())
    		columns.add(columnNames.getString("COLUMN_NAME"));
    	columnNames.close();
    	return columns;
    }
    
    /**
     * Gets all the foreign keys for a relation that reference another relation
     * @param forTable the referencing relation
     * @param toTable the referenced relation
     * @return A Set containing foreign keys from forTable that reference toTable
     * @throws SQLException
     */
    private Set<String> getForeignKeys(String forTable, String toTable) throws SQLException
    {
    	Set<String> fKeys = new HashSet<String>();
    	ResultSet fKeyNames = this.metaData.getImportedKeys(null, null, forTable);
    	while (fKeyNames.next())
    		if (fKeyNames.getString("PKTABLE_NAME").equalsIgnoreCase(toTable))
    			fKeys.add(fKeyNames.getString("PKCOLUMN_NAME"));
    	fKeyNames.close();
    	return fKeys;
    }
    
    /**
     * Finds the common column names between two relations
     * @param table1 the attributes of the first relation
     * @param table2 the attributes of the second relation
     * @return a Set containing the common attributes between the two relations
     */
    private Set<String> tableIntersect(final Set<String> table1, final Set<String> table2)
    {
    	Set<String> intersect = new HashSet<String>(table1);
    	intersect.retainAll(table2);
    	return intersect;
    }
    
    /**
     * Counts the number of rows in an individual relation
     * @param table the relation to count the number of rows for
     * @return the number of tuples in the relation
     * @throws SQLException
     */
    private int getRowCount(String table) throws SQLException
    {
    	final String QUERY = "SELECT COUNT(*) FROM " + table + ";";
    	Statement SQL = this.db.createStatement();
    	ResultSet count = SQL.executeQuery(QUERY);
    	count.next();
    	int rowCount = count.getInt("count");
    	count.close();
    	return rowCount;
    }
    
    /**
     * Checks if a set of attributes is a key for a relation by comparing the number of distinct rows it can select.
     * @param table the relation being checked
     * @param expected the number of rows in the relation
     * @param keyAttributes the attributes being checked as a key
     * @return true if the set of attributes can be a key for the relation
     * @throws SQLException
     */
    private boolean isKeyFor(final String table, final int expected, final Set<String> keyAttributes) throws SQLException
    {
    	if (keyAttributes.isEmpty())
    		return false;
    	StringBuilder sb = new StringBuilder();
    	Iterator<String> itr = keyAttributes.iterator();
    	while (itr.hasNext())
    	{
    		sb.append(itr.next());
    		if (itr.hasNext())
    			sb.append(", ");
    	}
    	final String QUERY = "SELECT DISTINCT COUNT(*) FROM (SELECT DISTINCT " + sb.toString() + " FROM " + table + ") AS keyCheck;";
    	Statement SQL = this.db.createStatement();
    	ResultSet count = SQL.executeQuery(QUERY);
    	count.next();
    	int keyRowCount = count.getInt("count");
    	count.close();
    	return keyRowCount == expected;
    }
    
    /**
     * Checks if the set of attributes is part of the set of foreign key attributes for a relation
     * @param table the foreign key attributes being checked against
     * @param attributes the attributes being checked
     * @return true if at least one of the attributes exists as part of the FK of the relation; false otherwise.
     */
    private boolean isFKeyFor(final Set<String> table, final Set<String> attributes)
    {
    	if (attributes.isEmpty())
    		return false;
    	return table.containsAll(attributes);
    }
    
    /**
     * Used for Case 4 to get the number of distinct rows of a given column
     * @param table the relation being projected over
     * @param column the column being projected
     * @return the count of the number of distinct rows in the projected column
     * @throws SQLException
     */
    private int getProjectionCount(String table, String column) throws SQLException
    {
    	final String QUERY = "SELECT COUNT(DISTINCT " + column + ") FROM " + table + ";";
    	Statement SQL = this.db.createStatement();
    	ResultSet count = SQL.executeQuery(QUERY);
    	count.next();
    	int projCount = count.getInt("count");
    	count.close();
    	return projCount;
    	
    }
    
    /**
     * Estimates the size of joining two relations based on four cases
     *      Case 1: If R intersect S = empty set
     *      Case 2: If R intersect S is a key for R
     *      Case 3: If R intersect S is a foreign key in S referencing R. R intersect S being a foreign key referencing S is symmetric.
     *      Case 4: If R intersect S is one attribute, {A}, and that attribute is not a key for either R or S
     * @return the estimates size of the join or -1 if it failed to accurately calculate it
     * @throws Exception
     */
    private int estimateJoinSize() throws Exception
    {
    	Set<String> RCols = this.getColumnNames(this.table1);						// all the columns in R
    	Set<String> RFK = this.getForeignKeys(this.table1, this.table2);			// the foreign keys from R -> S
    	int RRowCount = this.getRowCount(this.table1);								// the number of tuples in R
    	
    	Set<String> SCols = this.getColumnNames(this.table2);						// all the columns in S
    	Set<String> SFK = this.getForeignKeys(this.table2, this.table1);			// the foreign keys from S -> R
    	int SRowCount = this.getRowCount(this.table2);								// the number of tuples in S

    	Set<String> ColsIntersected = this.tableIntersect(RCols, SCols);			// R intersect S on column names
    	boolean isKeyForR = false;
    	int estimatedSize = -1;
    	
    	/**
    	 * Case 1: If R intersect S = empty set
    	 * Number of tuples in joined relation equal to r * s
    	 */
    	if (ColsIntersected.isEmpty())
    	{
    		this.joinCase = JoinCase.CASE1;
    		return RRowCount * SRowCount;
    	}
    	
    	/**
    	 * Case 2: If R intersect S is a key for R
    	 * Number of tuples in joined relation is no more than the number of tuples in S
    	 */
    	if (this.isKeyFor(this.table1, RRowCount, ColsIntersected))
    	{
    		this.joinCase = JoinCase.CASE2;
    		isKeyForR = true;
    		estimatedSize = SRowCount;
    	}
    	/**
    	 * Case 3: If R intersect S is a foreign key in S referencing R
    	 * Note: R intersect S being a foreign key referencing S is symmetric
    	 * Number of tuples in joined relation is exactly the same as the number of tuples in s.
    	 */
    	if (!isKeyForR)
    	{
    		// R intersect S}
    		if (this.isFKeyFor(SFK, ColsIntersected))
    		{
    			System.out.println("Case 3a");
    			this.joinCase = JoinCase.CASE3;
    			estimatedSize = SRowCount;
    		}
    		
    		// S intersect R
    		if (estimatedSize == -1)
    		{
    			if (this.isFKeyFor(RFK, ColsIntersected))
    			{
    				System.out.println("Case 3b");
    				this.joinCase = JoinCase.CASE3;
    				estimatedSize = RRowCount;
    			}
    			
    			// if estimated size is still -1 means the value still wasn't correctly estimated
    			// if R intersect S was one attribute
    			if (estimatedSize == -1 && ColsIntersected.size() == 1)
    			{
    				/**
    				 * Case 4: If R intersect S is one attribute, {A}, and that attribute is not a key for either R or S
    				 * Number of tuples in the joined relation is the minimum of
    				 *     (n_r * n_s) / V(A, R) and (n_r * n_s) / V(A, S)
    				 */
    				if (!this.isKeyFor(this.table2, SRowCount, ColsIntersected))
    				{
    					this.joinCase = JoinCase.CASE4;
    					String[] intersected = ColsIntersected.toArray(new String[ColsIntersected.size()]);
    					int RProjCount = this.getProjectionCount(this.table1, intersected[0]);
    					int SProjCount = this.getProjectionCount(this.table2, intersected[0]);
    					int estimatedR = (RRowCount * SRowCount) / RProjCount;
    					int estimatedS = (RRowCount * SRowCount) / SProjCount;
    					return Math.min(estimatedR, estimatedS);
    				}
    			}
    		}
    	}
    	return estimatedSize;
    }
    
    /**
     * Get the actual join size by executing the join on the two relations and returning the count of the number of tuples
     * @return the number of tuples in the joined relation
     * @throws SQLException
     */
    private int actualJoinSize() throws SQLException
    {
    	final String QUERY = "SELECT COUNT(*) FROM " + this.table1 + " NATURAL JOIN " + this.table2 + ";";
    	Statement SQL = this.db.createStatement();
    	ResultSet count = SQL.executeQuery(QUERY);
    	count.next();
    	int rowCount = count.getInt("count");
    	count.close();
    	return rowCount;
    }
    
    /**
     * Display the estimated join size, actual join size, and estimation error
     * @throws Exception
     */
    public void joinSize() throws Exception
    {
    	int estimated = this.estimateJoinSize();
    	int actual = this.actualJoinSize();
    	System.out.println("Cost to join \"" + this.table1 + "\" and \"" + this.table2 + "\":");
    	System.out.println("\tEstimated Join Size: " + estimated);
    	System.out.println("\tActual Join Size: " + actual);
    	System.out.println("\tEstimation Error: " + (estimated - actual));
    	
    	System.out.println();
    	switch(this.joinCase)
    	{
    		case NOJOIN:
    			System.out.println("Error estimating the size of joining these two relations.");
    			break;
    		case CASE1:
    			System.out.println("Case 1: If R intersect S = EMPTY_SET, the estimated join size is r x s.");
    			break;
    		case CASE2:
    			System.out.println("Case 2: If R intersect S is a key for R, the number of tuples in R join S is no greater than the number of tuples in S.");
    			break;
    		case CASE3:
    			System.out.println("Case 3: If R intersect S in S is a foreign key in S referencing R, then the number of tuples in R join S is exactly the same as the number of tuples in S.");
    			break;
    		case CASE4:
    			System.out.println("Case 4: If R intersect S = {A} is not a key for R or S, then the number of tuples in the joined relation is the minimum of {(n_r * n_s) / V(A, R), (n_r * n_s) / V(A, S)}.");
    			break;
    	}
    }
}