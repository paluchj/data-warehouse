package com.paluchj.examples.warehouse

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.web.reactive.server.WebTestClient
import spock.lang.Specification

@SpringBootTest(classes = DataWarehouseApp, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class BaseSpringBootSpecification extends Specification {

    @Autowired
    ApplicationContext applicationContext

    @Autowired
    WebTestClient webTestClient

}
