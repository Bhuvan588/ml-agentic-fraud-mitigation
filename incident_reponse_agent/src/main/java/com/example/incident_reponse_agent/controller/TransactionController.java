package com.example.incident_reponse_agent.controller;


import com.example.incident_reponse_agent.dto.TransactionRequest;
import com.example.incident_reponse_agent.service.IncidentProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final IncidentProducerService incidentProducerService;
    private final ObjectMapper objectMapper = new ObjectMapper();


    public TransactionController(IncidentProducerService incidentProducerService) {
        this.incidentProducerService = incidentProducerService;
    }

    @PostMapping("/process")
    public ResponseEntity<String> processTransaction(@RequestBody TransactionRequest transactionRequest)
    {

        try{
            String jsonPayload = objectMapper.writeValueAsString(transactionRequest);

            incidentProducerService.publishAlert(jsonPayload);

            return ResponseEntity.accepted().body("Transaction queued for real-time ML inference...");
        } catch (JacksonException e) {
            return ResponseEntity.internalServerError().body("Error processing transaction payload.");
        }


        //Hardcoding fraud roles as of now
//        if(transactionRequest.getAmount() > 50000  || "SuspiciousLocation".equals(transactionRequest.getLocation()))
//        {
//            String alertPayload = String.format(
//                    "{\"transactionId\":\"%s\",\"accountId\":\"%s\",\"amount\":%.2f,\"reason\":\"High amount anomaly\"}",
//                    transactionRequest.getTransactionId(), transactionRequest.getAccountId(), transactionRequest.getAmount()
//            );
//            incidentProducerService.publishAlert(alertPayload);
//            return ResponseEntity.accepted().body("Transaction flagged for review. Processing asynchronously.");
//        }
    }

    @PostMapping("/mitigate")
    public ResponseEntity<String> executeMitigation(@RequestBody String mitigationAction)
    {
        System.out.println("Action executed in core microservice: " + mitigationAction);
        return ResponseEntity.ok("Mitigation applied successfully");
    }
}
