package com.example.demo.controller;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.model.Tweet;
import com.example.demo.service.TweetService;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


@RestController
public class RestApiController {
	
	TweetService twService = new TweetService();
	@RequestMapping(value = "/hbase/scan/{rowKey}", method = RequestMethod.GET)
	public ResponseEntity<?> scanRowKey(@PathVariable("rowKey") String rowKey) throws JsonParseException, JsonMappingException, IOException {
		List<Cell> result = twService.scanRegexRowKey("test-kp", rowKey);
		Tweet tweet = new Tweet();
		ObjectMapper obj = new ObjectMapper();
		if (null==result) {
			System.out.println("result is null");
		}
		for (Cell cell : result) {
			String tempValue = String.valueOf(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			tweet = obj.readValue(tempValue, Tweet.class);
		}
		return new ResponseEntity<Tweet>(tweet, HttpStatus.OK);
	}
}
