package com.example.incident_reponse_agent.dto;

import lombok.Data;

@Data
public class TransactionRequest {
    private  String transactionId;
    private String accountId;
    private double amount;
    private String location;
    private String deviceId;
}
