package tech.pm.entities.raw.betDetails;

import lombok.Value;

import java.util.List;

@Value
public class ItemEventDetails {
  String id;
  String sportTypeKey;
  String categoryId;
  String tournamentId;
  String acceptedStartTime;
  String competitorType;
  List<String> competitorsIds;
  String type;
}
