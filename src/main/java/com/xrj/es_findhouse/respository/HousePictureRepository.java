package com.xrj.es_findhouse.respository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import com.xrj.es_findhouse.entity.HousePicture;

public interface HousePictureRepository extends CrudRepository<HousePicture, Long> {
    List<HousePicture> findAllByHouseId(Long id);
}
