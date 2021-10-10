package com.paluchj.examples.warehouse.convert

import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.bson.Document

import java.util.stream.Collectors

class CsvDataConverter {

    static List<Document> toMongoDocumentList(String csvData) {
        CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build()
        CsvMapper csvMapper = new CsvMapper()
        MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map).with(csvSchema).readValues(csvData)
        List<Map<?, ?>> list = mappingIterator.readAll()
        ObjectMapper objectMapper = new ObjectMapper()
        list.stream()
            .map(m -> objectMapper.writeValueAsString(m))
            .map(json -> Document.parse(json))
            .collect(Collectors.toUnmodifiableList())
    }
}
