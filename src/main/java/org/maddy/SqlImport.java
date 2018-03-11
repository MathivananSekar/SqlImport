package org.maddy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONObject;

public class SqlImport {

	public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException {
		String url = args[0];
		String user = args[1];
		String pwd = args[2];
		Class.forName(args[3]);
		String query = args[4];
		String tgtFile = args[5];
		Connection con = DriverManager.getConnection(url, user, pwd);
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		PrintWriter pw = new PrintWriter(new File(tgtFile));
		writeResultSetToWriter(rs, pw);
		pw.close();
		con.close();
	}

	public static void writeResultSetToWriter(ResultSet resultSet, PrintWriter writer) throws SQLException {
		ResultSetMetaData metadata = resultSet.getMetaData();
		int numColumns = metadata.getColumnCount();
		int numRows = 0;

		while (resultSet.next()) // iterate rows
		{
			++numRows;
			JSONObject obj = new JSONObject(); // extends HashMap
			for (int i = 1; i <= numColumns; ++i) // iterate columns
			{
				String column_name = metadata.getColumnName(i);
				obj.put(column_name, resultSet.getObject(column_name));
			}
			writer.println(obj.toString(0));

			if (numRows % 1000 == 0)
				writer.flush();
		}
	}
}
