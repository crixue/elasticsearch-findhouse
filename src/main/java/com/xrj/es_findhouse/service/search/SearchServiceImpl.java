package com.xrj.es_findhouse.service.search;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xrj.es_findhouse.entity.House;
import com.xrj.es_findhouse.entity.HouseDetail;
import com.xrj.es_findhouse.entity.HouseTag;
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
        
        SearchRequestBuilder requestBuilder = this.client.prepareSearch(INDEX_NAME)
        		.setTypes(INDEX_TYPE)
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
}
