package it.eng.anas.db;

import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.Utils;

public class ResultSetToJson {
	private ObjectMapper mapper = Utils.getMapper();
	
	public ArrayNode extract(ResultSet resultSet) throws Exception {
		ResultSetMetaData md = resultSet.getMetaData();
		int numCols = md.getColumnCount();
		List<String> colNames = IntStream.range(0, numCols)
		  .mapToObj(i -> {
		      try {
		          return md.getColumnName(i + 1);
		      } catch (Exception e) {
		          e.printStackTrace();
		          return "?";
		      }
		  })
		  .collect(Collectors.toList());

		ArrayNode result = mapper.createArrayNode();
		while (resultSet.next()) {
			ObjectNode row = mapper.createObjectNode();
		    colNames.forEach(cn -> {
		        try {
		        	JsonNode node = mapper.valueToTree(resultSet.getObject(cn));
		            row.set(cn, node);
		        } catch (Exception e) {
		            e.printStackTrace();
		        }
		    });
		    result.add(row);
		}
		return result;
	}
	
	private Pattern idpattern = Pattern.compile("[0-9A-F]{32}");
	public ArrayNode extractWithTypes(ResultSet resultSet,  HashMap<String, String> namemap) throws Exception {
		ResultSetMetaData md = resultSet.getMetaData();
		ArrayNode result = mapper.createArrayNode();
		int n = md.getColumnCount();

		while (resultSet.next()) {
			ObjectNode row = mapper.createObjectNode();
			for(int col=1; col<=n; col++) {
				String name = md.getColumnName(col);
				String mname = namemap.get(name.toLowerCase());
				if (mname==null)
					mname = name;
				
				String type = md.getColumnTypeName(col).toLowerCase();
				if (type.equals("string")|| (type.startsWith("varchar"))) {
					String val = resultSet.getString(col);
					if (val!=null)
						row.put(mname,  val);
				}
				else if (type.equals("raw")) {
					String val = resultSet.getString(col);
					if (val!=null) {
						if (idpattern.matcher(val).matches())
							val = FilenetDBHelper.dbid2guid(val);
						row.put(mname,  val);
					}
				}
				else if (type.equals("timestamp")) {
					java.sql.Timestamp val = resultSet.getTimestamp(col);
					if (val!=null)
					row.put(mname, new Date(val.getTime()).toString());
				}
				else if (type.startsWith("number")) {
					Double d = resultSet.getDouble(col);
					if (d!=null)
						row.put(mname, d.toString());
				}
				else if (type.startsWith("clob")) {
					Clob clob = resultSet.getClob(col);
					if (clob!=null) {
						Reader r = clob.getCharacterStream();
						String val = IOUtils.toString(r);
						if (val!=null) 
							row.put(mname, val);
					}
				}
				else {
					row.put(name, "tipo non supportato:"+type);
				}
			}
		    result.add(row);
		}
		return result;
	}

	public  ArrayNode extract(String sql, Connection con) throws Exception {
		SimpleDbOp op = new SimpleDbOp(con)
				.query(sql)
				.executeQuery()
				.throwOnError();
		ResultSet rs = op.getResultSet();
		ArrayNode result = extract(rs);
		op.close();
		return result;
	}

}
