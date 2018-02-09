package Hive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * JDBC操作Hive(Hive要提前启动HiveServer2)
 */
public class HiveJDBC {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://master:10000/test";
	private static String user = "root";
	private static String password = "";
	
	private static Connection conn = null;
	private static Statement stmt = null;
	private static ResultSet rs = null;
	
	//加载驱动 创建连接
	@Before
	public void init() throws ClassNotFoundException, SQLException {
		Class.forName(driverName);
		conn = DriverManager.getConnection(url, user, password);
		stmt = conn.createStatement();
	}
	
	//创建数据路
	@Test
	public void createDatabase() throws SQLException {
		String sql = "create database hive_test";
		System.out.println("running:" + sql );
		stmt.execute(sql);
	}
	
	
	// 删除数据库
	@Test
	public void dropDatabase() throws Exception {
		String sql = "drop database if exists hive_test";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}
	
	//获取所有数据库
	@Test
	public void showDatabases() throws SQLException {
		
		String sql = "show databases";
		System.out.println("running:" + sql );
		rs = stmt.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
	}
	
	@Test
	public void createTable() throws SQLException {
		StringBuilder sql = new StringBuilder("create table person(\n");
		sql.append("personid int,\n");
		sql.append("name string,\n");
		sql.append("age int\n");
		sql.append(")\n");
		sql.append("row format delimited fields terminated by '\\t'");
		System.out.println("running:" + sql );
		stmt.execute(sql.toString());
	}
	
	
	// 查询所有表
	@Test
	public void showTables() throws Exception {
		String sql = "show tables";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
	}
	
	// 查看表结构
	@Test
	public void descTable() throws Exception {
		String sql = "desc person";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}
	}
	
	
	//查看表结构
	@Test
	public void selectData() throws SQLException {
		String sql = "select * from person";
		System.out.println("running: " + sql);
		rs = stmt.executeQuery(sql);
		System.out.println();
		while (rs.next()) {
			System.out.println(rs.getString("id") + "\t\t" + rs.getString("name"));
		}
	}
	
	// 加载数据
	@Test
	public void loadData() throws Exception {
		String filePath = "/root/person";
		String sql = "load data local inpath '" + filePath + "' overwrite into table person";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}
	
	// 统计查询（会运行mapreduce作业）
	@Test
	public void countData() throws Exception {
		String sql = "select count(1) from person";
		System.out.println("Running: " + sql);
		rs = stmt.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getInt(1) );
		}
	}
	
	
	// 删除数据库表
	@Test
	public void deopTable() throws Exception {
		String sql = "drop table if exists personp";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}
	
	// 释放资源
	@After
	public void destory() throws Exception {
		if ( rs != null) {
			rs.close();
		}
		if (stmt != null) {
			stmt.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
	
	
}
