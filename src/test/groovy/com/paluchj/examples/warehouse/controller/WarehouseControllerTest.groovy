package com.paluchj.examples.warehouse.controller

import com.paluchj.examples.warehouse.BaseSpringBootSpecification
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.http.client.MultipartBodyBuilder
import org.springframework.web.reactive.function.BodyInserters

class WarehouseControllerTest extends BaseSpringBootSpecification {


    def 'should load all rows (23198) from default file when calling load_default endpoint'() {

        expect:
        webTestClient.post()
            .uri('/warehouse/load_default')
            .exchange()
            .expectStatus().isOk()
            .expectBody(String)
            .isEqualTo('Loaded rows: 23198')
    }

    def 'should load 13 rows from a given sample csv file when calling load endpoint'() {

        expect:
        webTestClient.post()
            .uri('/warehouse/load')
            .contentType(MediaType.MULTIPART_FORM_DATA)
            .body(BodyInserters.fromMultipartData(fromFile()))
            .exchange()
            .expectStatus().isOk()
            .expectBody(String)
            .isEqualTo('Loaded rows: 13')
    }

    def 'should calculate Impressions over time (daily)'() {

        given:
        webTestClient.post()
            .uri('/warehouse/load')
            .contentType(MediaType.MULTIPART_FORM_DATA)
            .body(BodyInserters.fromMultipartData(fromFile()))
            .exchange()
            .expectStatus().isOk()

        expect:
        webTestClient.post()
            .uri(uriBuilder ->
                                    uriBuilder
                                        .path('warehouse')
                                        .queryParam('aggregate', 'Impressions')
                                        .queryParam('group', 'Daily')
                                        .build())
                .exchange()
                .expectStatus().isOk()
        .expectBody()
        .jsonPath('$[0].count').exists()
        .jsonPath('$[0].Impressions_min').exists()
        .jsonPath('$[0].Impressions_max').exists()
        .jsonPath('$[0].Impressions_sum').exists()
        .jsonPath('$[0].group.Daily').exists()
        .jsonPath('$[0].Clicks_min').doesNotExist()
    }

    def 'should calculate Total Clicks per Datasource and Campaign'() {

        given:
        webTestClient.post()
                .uri('/warehouse/load')
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(fromFile()))
                .exchange()
                .expectStatus().isOk()

        expect:
        webTestClient.post()
                .uri(uriBuilder ->
                        uriBuilder
                                .path('warehouse')
                                .queryParam('aggregate', 'Clicks')
                                .queryParam('group', 'Datasource', 'Campaign')
                                .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath('$[0].count').exists()
                .jsonPath('$[0].Clicks_sum').exists()
                .jsonPath('$[0].group.Datasource').exists()
                .jsonPath('$[0].group.Campaign').exists()
                .jsonPath('$[0].Impressions_min').doesNotExist()
    }

    private def fromFile() {
        def builder = new MultipartBodyBuilder()
        builder.part('file', new ClassPathResource('sample.csv'))
        builder.build()
    }
}
