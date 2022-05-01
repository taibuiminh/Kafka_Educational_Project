package tech.pm.entities.core.betAcceptItem;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class BetAcceptItem {
  String betItemId;
  List<ItemJoinEvent> items;
}
