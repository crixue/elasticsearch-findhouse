package com.xrj.es_findhouse.respository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import com.xrj.es_findhouse.entity.SubwayStation;

public interface SubwayStationRepository extends CrudRepository<SubwayStation, Long> {
    List<SubwayStation> findAllBySubwayId(Long subwayId);
}
