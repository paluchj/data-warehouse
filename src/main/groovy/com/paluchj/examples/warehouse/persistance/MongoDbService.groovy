package com.paluchj.examples.warehouse.persistance

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Accumulators
import com.mongodb.client.model.Filters
import org.bson.Document
import org.springframework.stereotype.Service

import static com.mongodb.client.model.Aggregates.*
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Projections.*

@Service
class MongoDbService {

    static def DATABASE_NAME = 'warehouse'
    static def COLLECTION_NAME = 'data'

    private def getCollection() {
        MongoClient mongoClient = MongoClients.create()
        MongoDatabase mongoDatabase = mongoClient.getDatabase(DATABASE_NAME)
        mongoDatabase.getCollection(COLLECTION_NAME)
    }

    def insertDocuments(List<Document> jsons) {
        collection.drop()
        collection.insertMany(jsons)
        collection.countDocuments()
    }

    def query(Set<String> aggregate, Set<String> groupBy, String filter) {
        //TODO: make filter mechanism more user friendly
        collection.aggregate(Arrays.asList(
                buildMatchStage(filter),
                buildGroupStage(groupBy, aggregate),
                buildProjectStage(aggregate)
        )).collect()
    }

    private def buildGroupStage(Set<String> groupBy, Set<String> aggregate) {
        group(getFields(groupBy), getAccumulators(aggregate))
    }

    private def getEq(String field) {
        eq(field, toExpression(field))
    }

    private def getFields(Set<String> groupBy) {
        fields(groupBy.collect{getEq(it)})
    }

    private def getFieldNamesForMetric(String metric) {
        [
            "${metric}_max".toString(),
            "${metric}_min".toString(),
            "${metric}_sum".toString(),
            "${metric}_avg".toString()
        ]
    }

    private def aggregateFieldNames(Set<String> aggregate) {
        aggregate.collect{getFieldNamesForMetric(it)}.flatten() + 'count'
    }

    private def buildProjectStage(Set<String> aggregate) {
        project(fields(
                excludeId(),
                computed('group', '$_id'),
                include(aggregateFieldNames(aggregate))
        ))
    }

    private def getAccumulators(Set<String> aggregate) {
        aggregate.collect{getAccumulatorsForMetric(it)}.flatten() +
            Accumulators.sum('count', 1)
    }

    private def getAccumulatorsForMetric(String metric) {
        [
            Accumulators.max("${metric}_max", toExpression(metric)),
            Accumulators.min("${metric}_min", toExpression(metric)),
            Accumulators.sum("${metric}_sum", toExpression(metric)), //TODO: convert to int/long before sum
            Accumulators.avg("${metric}_avg", toExpression(metric)) //TODO: convert to int/long before sum
        ]
    }
    
    private def buildMatchStage(String filter) {
        !filter?.trim() ? match(Filters.empty()) : match(Document.parse(filter))
    }

    private String toExpression(String field) {
        "\$$field"
    }
}
