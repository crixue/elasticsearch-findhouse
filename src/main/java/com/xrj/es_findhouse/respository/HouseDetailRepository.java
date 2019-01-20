package com.xrj.es_findhouse.respository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import com.xrj.es_findhouse.entity.HouseDetail;


public interface HouseDetailRepository extends CrudRepository<HouseDetail, Long>{
    HouseDetail findByHouseId(Long houseId);

    List<HouseDetail> findAllByHouseIdIn(List<Long> houseIds);
}
