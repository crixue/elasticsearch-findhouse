package com.xrj.es_findhouse;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.xrj.es_findhouse.base.HouseSort;
import com.xrj.es_findhouse.base.ServiceMultiResult;
import com.xrj.es_findhouse.form.RentSearch;
import com.xrj.es_findhouse.service.search.HouseIndexMessage;
import com.xrj.es_findhouse.service.search.HouseIndexTemplate;
import com.xrj.es_findhouse.service.search.ISearchService;

public class SearchServiceTest extends ApplicationTests {

	@Autowired
	private ISearchService searchService;

	@Test
	public void testCUIndex() {
		for (int i = 15; i < 22; i++) {
			HouseIndexMessage message = new HouseIndexMessage();
			message.setHouseId(new Integer(i).longValue());
			boolean result =  searchService.createOrUpdateIndex(message);
			Assert.assertEquals(true, result);
		}
	}

	@Test
	public void testQuery() {
		RentSearch rentSearch = new RentSearch();
		rentSearch.setCityEnName("bj");
		rentSearch.setOrderBy(HouseSort.DEFAULT_SORT_KEY);
		rentSearch.setOrderDirection("ASC");
		rentSearch.setSize(10);
		rentSearch.setKeywords("商务 房子");
		ServiceMultiResult<Long> result = searchService.query(rentSearch);
		List<Long> houseIds = result.getResult();
		for (Long houseId : houseIds) {
			System.out.println(houseId);
		}
	}
	
	@Test
	public void testSuggest() {
		String prefix = "国";
		List<String> completions = searchService.suggestCompletion(prefix).getResult();
		for (String string : completions) {
			System.out.println(string);
		}
	}
	
	@Test
	public void testAggDistict() {
		HouseIndexTemplate indexTemplate = new HouseIndexTemplate();
		indexTemplate.setCityEnName("bj");
		indexTemplate.setRegionEnName("hdq");
		indexTemplate.setDistrict("融泽嘉园");
		long nums = searchService.aggregateDistrictHouse(indexTemplate).getResult();
		System.out.println(nums);
	}
	
}
