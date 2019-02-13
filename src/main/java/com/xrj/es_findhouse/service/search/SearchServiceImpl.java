package com.xrj.es_findhouse.service.search;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.xrj.es_findhouse.base.HouseSort;
import com.xrj.es_findhouse.base.HouseSuggest;
import com.xrj.es_findhouse.base.RentValueBlock;
import com.xrj.es_findhouse.base.ServiceMultiResult;
import com.xrj.es_findhouse.base.ServiceResult;
import com.xrj.es_findhouse.entity.House;
import com.xrj.es_findhouse.entity.HouseDetail;
import com.xrj.es_findhouse.entity.HouseTag;
import com.xrj.es_findhouse.form.RentSearch;
import com.xrj.es_findhouse.respository.HouseDetailRepository;
import com.xrj.es_findhouse.respository.HouseRepository;
import com.xrj.es_findhouse.respository.HouseTagRepository;
import com.xrj.es_findhouse.respository.SupportAddressRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SearchServiceImpl implements ISearchService {

	private static final String INDEX_NAME = "xunwu";

	private static final String INDEX_TYPE = "house";

	@Autowired
	private HouseRepository houseRepository;

	@Autowired
	private HouseDetailRepository houseDetailRepository;

	@Autowired
	private HouseTagRepository tagRepository;

	@Autowired
	private SupportAddressRepository supportAddressRepository;

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private TransportClient client;

	@Autowired
	private ObjectMapper objectMapper;

	@Override
	public boolean createOrUpdateIndex(HouseIndexMessage message) {
		Long houseId = message.getHouseId();

		House house = houseRepository.findOne(houseId);
		if (house == null) {
			log.error("Index house {} dose not exist!", houseId);
			return false;
		}

		HouseIndexTemplate indexTemplate = new HouseIndexTemplate();
		modelMapper.map(house, indexTemplate);

		HouseDetail detail = houseDetailRepository.findByHouseId(houseId);
		if (detail == null) {
			// TODO 异常情况
			return false;
		}

		modelMapper.map(detail, indexTemplate);
		List<HouseTag> tags = tagRepository.findAllByHouseId(houseId);
		if (tags != null && !tags.isEmpty()) {
			List<String> tagStrings = new ArrayList<>();
			tags.forEach(houseTag -> tagStrings.add(houseTag.getName()));
			indexTemplate.setTags(tagStrings);
		}

		SearchRequestBuilder requestBuilder = this.client.prepareSearch(INDEX_NAME).setTypes(INDEX_TYPE)
				.setQuery(QueryBuilders.termQuery(HouseIndexKey.HOUSE_ID, indexTemplate.getHouseId()));
		log.debug(requestBuilder.toString());
		SearchResponse response = requestBuilder.get();

		boolean success;
		long totalHit = response.getHits().getTotalHits();
		if (totalHit == 0) {
			success = create(indexTemplate);
		} else if (totalHit == 1) {
			String esId = response.getHits().getAt(0).getId();
			success = update(esId, indexTemplate);
		} else {
			success = deleteAndCreate(totalHit, indexTemplate);
		}
		return success;
	}

	private boolean create(HouseIndexTemplate indexTemplate) {
		boolean suggestSuccess = updateSuggest(indexTemplate);
		if (!suggestSuccess) {
			return suggestSuccess;
		}
		try {
			IndexResponse response = this.client.prepareIndex(INDEX_NAME, INDEX_TYPE)
					.setSource(objectMapper.writeValueAsBytes(indexTemplate), XContentType.JSON).get();
			log.debug("create index with house:{}", indexTemplate.getHouseId());
			if (response.status() == RestStatus.CREATED) {
				return true;
			}
			return false;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

	}

	private boolean update(String esId, HouseIndexTemplate indexTemplate) {
		boolean suggestSuccess = updateSuggest(indexTemplate);
		if (!suggestSuccess) {
			return suggestSuccess;
		}
		try {
			UpdateResponse response = this.client.prepareUpdate(INDEX_NAME, INDEX_TYPE, esId)
					.setDoc(objectMapper.writeValueAsBytes(indexTemplate), XContentType.JSON).get();
			log.debug("update index with house:{}", indexTemplate.getHouseId());
			if (response.status() == RestStatus.OK) {
				return true;
			}
			return false;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

	}

	private boolean deleteAndCreate(long totalHit, HouseIndexTemplate indexTemplate) {
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.matchQuery(HouseIndexKey.HOUSE_ID, indexTemplate.getHouseId())).source(INDEX_NAME)
				.get();
		long deleted = response.getDeleted();
		if (deleted != totalHit) {
			log.warn("Need delete {}, but {} was deleted!", totalHit, deleted);
			return false;
		} else {
			return create(indexTemplate);
		}
	}

	@Override
	public ServiceMultiResult<Long> query(RentSearch rentSearch) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

		if (!StringUtils.isEmpty(rentSearch.getCityEnName())) {
			boolQuery.filter(QueryBuilders.termQuery(HouseIndexKey.CITY_EN_NAME, rentSearch.getCityEnName()));
		}
		if (!StringUtils.isEmpty(rentSearch.getRegionEnName())) {
			boolQuery.filter(QueryBuilders.termQuery(HouseIndexKey.REGION_EN_NAME, rentSearch.getRegionEnName()));	
		}

		RentValueBlock priceBlock = RentValueBlock.matchPrice(rentSearch.getPriceBlock());
		if (!priceBlock.equals(RentValueBlock.ALL)) {
			RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(HouseIndexKey.PRICE);
			if (priceBlock.getMin() > 0) {
				rangeQueryBuilder.gte(priceBlock.getMin());
			}
			if (priceBlock.getMax() > 0) {
				rangeQueryBuilder.lte(priceBlock.getMax());
			}
			boolQuery.filter(rangeQueryBuilder);
		}

		RentValueBlock areaBlock = RentValueBlock.matchArea(rentSearch.getAreaBlock());
		if (!areaBlock.equals(RentValueBlock.ALL)) {
			RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(HouseIndexKey.AREA);
			if (areaBlock.getMin() > 0) {
				rangeQueryBuilder.gte(areaBlock.getMin());
			}
			if (priceBlock.getMax() > 0) {
				rangeQueryBuilder.lte(areaBlock.getMax());
			}
			boolQuery.filter(rangeQueryBuilder);
		}

		if (rentSearch.getDirection() > 0) {
			boolQuery.filter(QueryBuilders.termQuery(HouseIndexKey.DIRECTION, rentSearch.getDirection()));
		}
		if (rentSearch.getRentWay() > -1) {
			boolQuery.filter(QueryBuilders.termQuery(HouseIndexKey.DISTANCE_TO_SUBWAY, rentSearch.getRentWay()));
		}

		if (!StringUtils.isEmpty(rentSearch.getKeywords())) {
			boolQuery.must(QueryBuilders.multiMatchQuery(rentSearch.getKeywords(), HouseIndexKey.TITLE,
					HouseIndexKey.TRAFFIC, HouseIndexKey.DISTRICT, HouseIndexKey.ROUND_SERVICE,
					HouseIndexKey.SUBWAY_LINE_NAME, HouseIndexKey.SUBWAY_STATION_NAME));
		}

		SearchRequestBuilder requestBuilder = this.client.prepareSearch(INDEX_NAME).setTypes(INDEX_TYPE)
				.setQuery(boolQuery)
				.addSort(HouseSort.getSortKey(rentSearch.getOrderBy()),
						SortOrder.fromString(rentSearch.getOrderDirection()))
				.setFrom(rentSearch.getStart()).setSize(rentSearch.getSize())
				.setFetchSource(HouseIndexKey.HOUSE_ID, null);
		log.debug(requestBuilder.toString());

		List<Long> houseIds = new ArrayList<>();
		SearchResponse response = requestBuilder.get();
		if (response.status() != RestStatus.OK) {
			log.warn("search status is not ok:{}", requestBuilder.toString());
			return new ServiceMultiResult<Long>(0, houseIds);
		}
		
		for(SearchHit hit: response.getHits()) {
			log.debug("get hit response:{}", hit.getSourceAsString());
			houseIds.add(Longs.tryParse(String.valueOf(hit.getSourceAsMap().get(HouseIndexKey.HOUSE_ID))));
		}
		return new ServiceMultiResult<Long>(response.getHits().getTotalHits(), houseIds);
	}
	
	
	/**
	 * 更新补全建议词组
	 * @param houseIndexTemplate
	 * @return
	 */
	private boolean updateSuggest(HouseIndexTemplate houseIndexTemplate) {
		AnalyzeRequestBuilder requestBuilder = new AnalyzeRequestBuilder(this.client,
				AnalyzeAction.INSTANCE, INDEX_NAME, houseIndexTemplate.getTitle(),
				houseIndexTemplate.getLayoutDesc(), houseIndexTemplate.getRoundService(),
                houseIndexTemplate.getDescription(), houseIndexTemplate.getSubwayLineName(),
                houseIndexTemplate.getSubwayStationName());  //构建补全建议关键词，可以使用多个string字段
		requestBuilder.setAnalyzer("ik_smart");  //ik分词
		
		AnalyzeResponse response = requestBuilder.get();
		// Suggesters基本的运作原理是将输入的文本分解为token，然后在索引的字典里查找相似的term并返回
		List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();
        if (tokens == null) {
            log.warn("Can not analyze token for house: " + houseIndexTemplate.getHouseId());
            return false;
        }
		
        
        List<HouseSuggest> suggests = new ArrayList<HouseSuggest>();
        for (AnalyzeToken token : tokens) {
        	if (token.getType().equals("<NUM>") || token.getTerm().length() < 2) {  // 排除数字类型和小于两个字符的分词结果
        		continue;
			}
			
        	HouseSuggest houseSuggest = new HouseSuggest();
        	houseSuggest.setInput(token.getTerm());
        	suggests.add(houseSuggest);
		}
		
        //举例:定制化小区名称加入自动补全suggester
        if (!StringUtils.isEmpty(houseIndexTemplate.getDistrict())) {
			HouseSuggest suggest = new HouseSuggest();
			suggest.setInput(houseIndexTemplate.getDistrict());
			suggest.setWeight(houseIndexTemplate.getWeight());
			suggests.add(suggest);
		}
        
        houseIndexTemplate.setSuggest(suggests);
        return true;
	}
	
	/**
	 * 
"suggest": {
    "blog-suggest": [
      {
        "text": "elastic i",
        "offset": 0,
        "length": 9,
        "options": [
          {
            "text": "Elastic is the company behind ELK stack",
            "_index": "blogs_completion",
            "_type": "tech",
            "_id": "AVrXFyn-cpYmMpGqDdcd",
            "_score": 1,
            "_source": {
              "body": "Elastic is the company behind ELK stack"
            }
          }
        ]
      }
    ]
  }
	 * @param prefix
	 * @return
	 */
	@Override
	public ServiceResult<List<String>> suggestCompletion(String prefix) {
		String indexSuggestfieldName = "suggest";  //index中type为completion的字段
		CompletionSuggestionBuilder suggestion = SuggestBuilders.completionSuggestion(indexSuggestfieldName)
				.prefix(prefix).size(5).skipDuplicates(true);
		SuggestBuilder suggestBuilder = new SuggestBuilder();
		suggestBuilder.addSuggestion("autocomplete", suggestion);
		
		SearchRequestBuilder requestBuilder = this.client.prepareSearch(INDEX_NAME)
				.setTypes(INDEX_TYPE)
				.suggest(suggestBuilder);
		log.debug(requestBuilder.toString());
		
		SearchResponse response = requestBuilder.get();
		Suggest suggest = response.getSuggest();
		if (suggest == null) {
			return ServiceResult.of(new ArrayList<>());
		}
		
		Suggestion resultSuggestion = suggest.getSuggestion("autocomplete");
		
        int maxSuggest = 0;
        Set<String> suggestSet = new HashSet<>();
        
        for(Object term: resultSuggestion.getEntries()) {
        	if (term instanceof CompletionSuggestion.Entry) {
				CompletionSuggestion.Entry item = (Entry) term;
        		
				if (item.getOptions().isEmpty()) {
					continue;
				}
				
				for(CompletionSuggestion.Entry.Option option: item) {
					String tipString = option.getText().string();
					if (suggestSet.contains(tipString)) {
						continue;
					}
					suggestSet.add(tipString);
					maxSuggest ++;
				}
				if (maxSuggest > 5) {
					break;
				}
			}
        	
        }
        List<String> suggests = Lists.newArrayList(suggestSet.toArray(new String[]{}));
        return ServiceResult.of(suggests);
	}
	
}
