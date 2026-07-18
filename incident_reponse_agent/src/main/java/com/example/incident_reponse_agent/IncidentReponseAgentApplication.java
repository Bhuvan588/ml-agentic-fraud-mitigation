package com.example.incident_reponse_agent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class IncidentReponseAgentApplication {

	public static void main(String[] args) {
		SpringApplication.run(IncidentReponseAgentApplication.class, args);
	}

}
