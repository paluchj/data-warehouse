package com.paluchj.examples.warehouse.controller

import com.paluchj.examples.warehouse.service.WarehouseService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.multipart.MultipartFile

@RestController
@RequestMapping('warehouse')
class WarehouseController {

    @Autowired
    WarehouseService warehouseService

    @PostMapping
    def query(@RequestParam Set<String> aggregate,
              @RequestParam(required = false) Set<String> group,
              @RequestParam(required = false) String filter) {
        warehouseService.queryData(aggregate, group, filter)
    }

    @PostMapping(path = 'load_default')
    def loadDefault() {
        warehouseService.saveDefaultData()
    }

    @PostMapping(path = 'load')
    def loadData(@RequestParam('file') MultipartFile file) {
        warehouseService.saveData(file)
    }
}