package com.paluchj.examples.warehouse.service

import com.paluchj.examples.warehouse.convert.CsvDataConverter
import com.paluchj.examples.warehouse.persistance.MongoDbService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.web.multipart.MultipartFile

import java.util.stream.Collectors

@Service
class WarehouseService {

    @Autowired
    MongoDbService mongoDbService

    def saveDefaultData() {
        def inputData = readDefaultDataFromResources()
        loadDataToMongoDb(inputData)
    }

    def saveData(MultipartFile file) {
        def inputData = new String(file.bytes) //TODO: do this in more efficient way
        loadDataToMongoDb(inputData)
    }

    def queryData(Set<String> aggregate, Set<String> group, String filter) {
        mongoDbService.query(aggregate, group, filter)
    }

    private def loadDataToMongoDb(String inputData) {
        'Loaded rows: ' + mongoDbService.insertDocuments(CsvDataConverter.toMongoDocumentList(inputData))
    }

    private def readDefaultDataFromResources() {
        InputStream is = WarehouseService.getResourceAsStream("/data.csv")
        BufferedReader reader = new BufferedReader(new InputStreamReader(is))
        reader.lines().collect(Collectors.joining(System.lineSeparator()))
    }

}
