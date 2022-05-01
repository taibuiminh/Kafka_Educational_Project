package tech.pm.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import tech.pm.entities.raw.Bet;
import tech.pm.entities.core.betAcceptItem.BetAcceptItem;
import tech.pm.entities.core.betAcceptPlayer.BetAcceptPlayer;
import tech.pm.entities.core.BetAccepted;
import tech.pm.entities.raw.Email;
import tech.pm.entities.raw.Event;
import tech.pm.entities.raw.betDetails.Item;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;
import tech.pm.entities.raw.betDetails.ModifiedItem;
import tech.pm.entities.core.betAcceptPlayer.Player;
import tech.pm.entities.core.betAcceptPlayer.PlayerEmailVerified;
import tech.pm.entities.raw.PlayerProfile;
import tech.pm.entities.raw.TestPlayer;
import tech.pm.serdes.deserializer.BetAcceptItemDeserializer;
import tech.pm.serdes.deserializer.BetAcceptPlayerDeserializer;
import tech.pm.serdes.deserializer.BetAcceptedDeserializer;
import tech.pm.serdes.deserializer.BetDeserializer;
import tech.pm.serdes.deserializer.EmailDeserializer;
import tech.pm.serdes.deserializer.EventDeserializer;
import tech.pm.serdes.deserializer.ItemDeserializer;
import tech.pm.serdes.deserializer.ItemJoinEventDeserializer;
import tech.pm.serdes.deserializer.ItemJoinEventListDeserializer;
import tech.pm.serdes.deserializer.ModifiedItemDeserializer;
import tech.pm.serdes.deserializer.PlayerDeserializer;
import tech.pm.serdes.deserializer.PlayerEmailVerifiedDeserializer;
import tech.pm.serdes.deserializer.PlayerProfileDeserializer;
import tech.pm.serdes.deserializer.TestPlayerDeserializer;
import tech.pm.serdes.serializer.BetAcceptItemSerializer;
import tech.pm.serdes.serializer.BetAcceptPlayerSerializer;
import tech.pm.serdes.serializer.BetAcceptedSerializer;
import tech.pm.serdes.serializer.BetSerializer;
import tech.pm.serdes.serializer.EmailSerializer;
import tech.pm.serdes.serializer.EventSerializer;
import tech.pm.serdes.serializer.ItemJoinEventListSerializer;
import tech.pm.serdes.serializer.ItemJoinEventSerializer;
import tech.pm.serdes.serializer.ItemSerializer;
import tech.pm.serdes.serializer.ModifiedItemSerializer;
import tech.pm.serdes.serializer.PlayerEmailVerifiedSerializer;
import tech.pm.serdes.serializer.PlayerProfileSerializer;
import tech.pm.serdes.serializer.PlayerSerializer;
import tech.pm.serdes.serializer.TestPlayerSerializer;

import java.util.List;

public final class CustomSerde {

  public static Serde<PlayerProfile> PlayerProfile() {
    return new PlayerProfileSerde();
  }

  public static Serde<Event> Event() {
    return new EventSerde();
  }

  public static Serde<Bet> Bet() {
    return new BetSerde();
  }

  public static Serde<Email> Email() {
    return new EmailSerde();
  }

  public static Serde<TestPlayer> TestPlayer() {
    return new TestPlayerSerde();
  }

  public static Serde<PlayerEmailVerified> PlayerEmailVerified() {
    return new PlayerEmailVerifiedSerde();
  }

  public static Serde<Player> Player() {
    return new PlayerSerde();
  }

  public static Serde<BetAcceptPlayer> BetAcceptPlayer() {
    return new BetAcceptPlayerSerde();
  }

  public static Serde<Item> Item() {
    return new ItemSerde();
  }

  public static Serde<ItemJoinEvent> ItemJoinEvent() {
    return new ItemJoinEventSerde();
  }

  public static Serde<ModifiedItem> ModifiedItem() {
    return new ModifiedItemSerde();
  }

  public static Serde<List<ItemJoinEvent>> ItemJoinEventList() {
    return new ItemJoinEventListSerde();
  }

  public static Serde<BetAcceptItem> BetAcceptItem() {
    return new BetAcceptItemSerde();
  }

  public static Serde<BetAccepted> BetAccepted() {
    return new BetAcceptedSerde();
  }

  static public final class PlayerProfileSerde extends Serdes.WrapperSerde<PlayerProfile> {
    public PlayerProfileSerde() {
      super(new PlayerProfileSerializer(), new PlayerProfileDeserializer());
    }
  }

  static public final class EventSerde extends Serdes.WrapperSerde<Event> {
    public EventSerde() {
      super(new EventSerializer(), new EventDeserializer());
    }
  }

  static public final class BetSerde extends Serdes.WrapperSerde<Bet> {
    public BetSerde() {
      super(new BetSerializer(), new BetDeserializer());
    }
  }

  static public final class EmailSerde extends Serdes.WrapperSerde<Email> {
    public EmailSerde() {
      super(new EmailSerializer(), new EmailDeserializer());
    }
  }

  static public final class TestPlayerSerde extends Serdes.WrapperSerde<TestPlayer> {
    public TestPlayerSerde() {
      super(new TestPlayerSerializer(), new TestPlayerDeserializer());
    }
  }

  static public final class PlayerEmailVerifiedSerde extends Serdes.WrapperSerde<PlayerEmailVerified> {
    public PlayerEmailVerifiedSerde() {
      super(new PlayerEmailVerifiedSerializer(), new PlayerEmailVerifiedDeserializer());
    }
  }

  static public final class PlayerSerde extends Serdes.WrapperSerde<Player> {
    public PlayerSerde() {
      super(new PlayerSerializer(), new PlayerDeserializer());
    }
  }

  static public final class BetAcceptPlayerSerde extends Serdes.WrapperSerde<BetAcceptPlayer> {
    public BetAcceptPlayerSerde() {
      super(new BetAcceptPlayerSerializer(), new BetAcceptPlayerDeserializer());
    }
  }

  static public final class BetAcceptItemSerde extends Serdes.WrapperSerde<BetAcceptItem> {
    public BetAcceptItemSerde() {
      super(new BetAcceptItemSerializer(), new BetAcceptItemDeserializer());
    }
  }

  static public final class ItemJoinEventSerde extends Serdes.WrapperSerde<ItemJoinEvent> {
    public ItemJoinEventSerde() {
      super(new ItemJoinEventSerializer(), new ItemJoinEventDeserializer());
    }
  }

  static public final class ModifiedItemSerde extends Serdes.WrapperSerde<ModifiedItem> {
    public ModifiedItemSerde() {
      super(new ModifiedItemSerializer(), new ModifiedItemDeserializer());
    }
  }

  static public final class ItemSerde extends Serdes.WrapperSerde<Item> {
    public ItemSerde() {
      super(new ItemSerializer(), new ItemDeserializer());
    }
  }

  static public final class ItemJoinEventListSerde extends Serdes.WrapperSerde<List<ItemJoinEvent>> {
    public ItemJoinEventListSerde() {
      super(new ItemJoinEventListSerializer(), new ItemJoinEventListDeserializer());
    }
  }

  static public final class BetAcceptedSerde extends Serdes.WrapperSerde<BetAccepted> {
    public BetAcceptedSerde() {
      super(new BetAcceptedSerializer(), new BetAcceptedDeserializer());
    }
  }


}
