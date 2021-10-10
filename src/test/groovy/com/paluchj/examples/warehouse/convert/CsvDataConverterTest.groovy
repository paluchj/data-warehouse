package com.paluchj.examples.warehouse.convert

import spock.lang.Specification

class CsvDataConverterTest extends Specification {

    def 'should convert sample csv data into mongo db document'() {
        given:
        def sampleCsvData = 'dimension1,dimension2,metric\n' +
                            'input,data,1'

        when:
        def result = CsvDataConverter.toMongoDocumentList(sampleCsvData)

        then:
        result.size() == 1
        result.get(0).get('dimension1') == 'input'
        result.get(0).get('dimension2') == 'data'
        result.get(0).get('metric') == '1'
    }
}
