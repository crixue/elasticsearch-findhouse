package com.xrj.es_findhouse.respository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import com.xrj.es_findhouse.entity.Subway;

public interface SubwayRepository extends CrudRepository<Subway, Long>{
    List<Subway> findAllByCityEnName(String cityEnName);
}
