package com.paluchj.examples.warehouse.persistance

import com.paluchj.examples.warehouse.BaseSpringBootSpecification
import com.paluchj.examples.warehouse.convert.CsvDataConverter
import org.springframework.beans.factory.annotation.Autowired

import java.util.stream.Collectors

class MongoDbServiceTest extends BaseSpringBootSpecification {

    @Autowired
    MongoDbService mongoDbService

    def 'should insert document into mongo db for given csv'() {

        given: 'csv data with one row - one document'
        def csvData = 'dimension1,dimension2,metric\n' +
                'input,data,1'
        def documents = CsvDataConverter.toMongoDocumentList(csvData)

        when: 'documents are inserted to mongo db'
        def result = mongoDbService.insertDocuments(documents)

        then: 'result should contain number of inserted documents'
        result == 1
    }

    def 'should calculate Impressions data for "Google Ads" Datasource'() {

        given:
        def csvData = getResourceAsString('/sample.csv')
        mongoDbService.insertDocuments(CsvDataConverter.toMongoDocumentList(csvData))

        when:
        def result = mongoDbService.query(['Impressions'] as Set, ['Datasource'] as Set, '{ Datasource: "Google Ads" }')

        then:
        result.size() == 1
        result.get(0).get('group')?.get('Datasource') == 'Google Ads'
        result.get(0).get('Impressions_min') == "17079"
        result.get(0).get('Impressions_max') == "80351"
        result.get(0).get('count') == 7
    }

    private def getResourceAsString(String name) {
        InputStream is = MongoDbServiceTest.getResourceAsStream(name)
        BufferedReader reader = new BufferedReader(new InputStreamReader(is))
        reader.lines().collect(Collectors.joining(System.lineSeparator()))
    }

}
