package com.xrj.es_findhouse;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.xrj.es_findhouse.service.search.HouseIndexMessage;
import com.xrj.es_findhouse.service.search.ISearchService;

public class SearchServiceTest extends ApplicationTests {

	@Autowired
	private ISearchService searchService;

	@Test
	public void testCUIndex() {
		Long targetHouseId = 15L;

		HouseIndexMessage message = new HouseIndexMessage();
		message.setHouseId(15L);
		boolean result =  searchService.createOrUpdateIndex(message);
		Assert.assertEquals(true, result);

	}

}
