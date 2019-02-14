package com.xrj.es_findhouse.service.search;

import java.util.List;

import com.xrj.es_findhouse.base.ServiceMultiResult;
import com.xrj.es_findhouse.base.ServiceResult;
import com.xrj.es_findhouse.form.RentSearch;

public interface ISearchService {

	boolean createOrUpdateIndex(HouseIndexMessage message);

	ServiceMultiResult<Long> query(RentSearch rentSearch);

	/**
	 * 获取补全建议关键词
	 * @param prefix
	 * @return
	 */
	ServiceResult<List<String>> suggestCompletion(String prefix);

	/**
	 * 获取指定city-region-小区的聚合数目
	 * @param indexTemplate
	 * @return
	 */
	ServiceResult<Long> aggregateDistrictHouse(HouseIndexTemplate indexTemplate);

}
