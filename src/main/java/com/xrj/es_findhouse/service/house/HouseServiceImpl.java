package com.xrj.es_findhouse.service.house;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.criteria.Predicate;

import org.elasticsearch.client.transport.TransportClient;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.xrj.es_findhouse.base.HouseSort;
import com.xrj.es_findhouse.base.HouseStatus;
import com.xrj.es_findhouse.base.ServiceMultiResult;
import com.xrj.es_findhouse.dto.HouseDTO;
import com.xrj.es_findhouse.dto.HouseDetailDTO;
import com.xrj.es_findhouse.entity.House;
import com.xrj.es_findhouse.entity.HouseDetail;
import com.xrj.es_findhouse.entity.HouseTag;
import com.xrj.es_findhouse.form.RentSearch;
import com.xrj.es_findhouse.respository.HouseDetailRepository;
import com.xrj.es_findhouse.respository.HouseRepository;
import com.xrj.es_findhouse.respository.HouseTagRepository;
import com.xrj.es_findhouse.respository.SupportAddressRepository;

@Service
public class HouseServiceImpl implements IHouseService {

	@Autowired
	private HouseRepository houseRepository;

	@Autowired
	private HouseDetailRepository houseDetailRepository;

	@Autowired
	private HouseTagRepository tagRepository;

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private ObjectMapper objectMapper;

	private ServiceMultiResult<HouseDTO> simpleQuery(RentSearch rentSearch) {
		Sort sort = HouseSort.generateSort(rentSearch.getOrderBy(), rentSearch.getOrderDirection());
		int page = rentSearch.getStart() / rentSearch.getSize();

		Pageable pageable = new PageRequest(page, rentSearch.getSize(), sort);

		Specification<House> specification = (root, criteriaQuery, criteriaBuilder) -> {
			Predicate predicate = criteriaBuilder.equal(root.get("status"), HouseStatus.PASSES.getValue());

			predicate = criteriaBuilder.and(predicate,
					criteriaBuilder.equal(root.get("cityEnName"), rentSearch.getCityEnName()));

			if (HouseSort.DISTANCE_TO_SUBWAY_KEY.equals(rentSearch.getOrderBy())) {
				predicate = criteriaBuilder.and(predicate,
						criteriaBuilder.gt(root.get(HouseSort.DISTANCE_TO_SUBWAY_KEY), -1));
			}
			return predicate;
		};

		Page<House> houses = houseRepository.findAll(specification, pageable);
		List<HouseDTO> houseDTOS = new ArrayList<>();

		List<Long> houseIds = new ArrayList<>();
		Map<Long, HouseDTO> idToHouseMap = Maps.newHashMap();
		houses.forEach(house -> {
			HouseDTO houseDTO = modelMapper.map(house, HouseDTO.class);
//            houseDTO.setCover(this.cdnPrefix + house.getCover());
			houseDTO.setCover(house.getCover());
			houseDTOS.add(houseDTO);

			houseIds.add(house.getId());
			idToHouseMap.put(house.getId(), houseDTO);
		});

		wrapperHouseList(houseIds, idToHouseMap);
		return new ServiceMultiResult<>(houses.getTotalElements(), houseDTOS);
	}

	/**
	 * 渲染详细信息 及 标签
	 * 
	 * @param houseIds
	 * @param idToHouseMap
	 */
	private void wrapperHouseList(List<Long> houseIds, Map<Long, HouseDTO> idToHouseMap) {
		List<HouseDetail> details = houseDetailRepository.findAllByHouseIdIn(houseIds);
		details.forEach(houseDetail -> {
			HouseDTO houseDTO = idToHouseMap.get(houseDetail.getHouseId());
			HouseDetailDTO detailDTO = modelMapper.map(houseDetail, HouseDetailDTO.class);
			houseDTO.setHouseDetail(detailDTO);
		});

		List<HouseTag> houseTags = tagRepository.findAllByHouseIdIn(houseIds);
		houseTags.forEach(houseTag -> {
			HouseDTO house = idToHouseMap.get(houseTag.getHouseId());
			house.getTags().add(houseTag.getName());
		});
	}
}
