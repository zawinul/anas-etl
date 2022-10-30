package it.eng.anas.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
	
	public  ArrayNode extract(String sql) throws Exception {
		SimpleDbOp op = new SimpleDbOp()
				.query(sql)
				.executeQuery()
				.throwOnError();
		ResultSet rs = op.getResultSet();
		ArrayNode result = extract(rs);
		op.close();
		return result;
	}

}
