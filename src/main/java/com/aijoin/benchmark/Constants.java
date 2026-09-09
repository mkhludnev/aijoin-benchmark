package com.aijoin.benchmark;

import java.util.List;

/**
 * Shared collection names, field names, and data-generation parameters. The indexer and searcher
 * must agree on all of this since the searcher never queries the index to discover what's there --
 * it just knows, from these constants, what values exist to filter on.
 */
final class Constants {
    public static final String PRODSKUS_COLLECTION = "products_skus" ;
  public static final String PRODUCT_ID_NUM = "ID_NUM";
  public static final String PRODUCT_ID_FK_NUM = "PRODUCT_ID_FK_NUM";

  private Constants() {}

  static final String PRODUCTS_COLLECTION = "products";
  static final String SKUS_COLLECTION = "skus";

  // products fields
  static final String PRODUCT_ID = "ID";
  static final String TITLE = "title";
  static final String BRAND = "brand";

  // skus fields
  static final String SKU_ID = "ID";
  static final String PRODUCT_ID_FK = "PRODUCT_ID_FK";
  static final String COLOR_KEYWORD = "color_s";
  static final String COLOR_TEXT = "color_t";
  static final String SIZE_KEYWORD = "size_s";
  static final String SIZE_TEXT = "size_t";
  static final String INVENTORY_STOCK = "inventory_stock";

  static final int PRODUCT_COUNT = 1_000_000;
  static final int SKU_COUNT = 10_000_000;

  /** Deterministic seed: indexer and searcher both derive their RNGs from this. */
  static final long RANDOM_SEED = 20260712L;

  static final int INDEXER_BATCH_SIZE = 500;
  static final int INDEXER_THREADS = 4;

  static final int MAX_INVENTORY_STOCK = 10_000;

  // vocabularies the indexer draws from and the searcher filters on
  static final List<String> BRANDS =
      List.of(
          "Acme", "Umbra", "Nimbus", "Vertex", "Solace", "Kinetic", "Halcyon", "Zenith", "Quartz",
          "Meridian", "Cobalt", "Ember", "Lumen", "Tundra", "Voxel", "Prairie", "Anchor",
          "Beacon", "Cascade", "Drift", "Ridgeline", "Summit", "Harbor", "Frontier", "Compass",
          "Wayfarer", "Northstar", "Basalt", "Granite", "Obsidian");

  static final List<String> COLORS =
      List.of(
          "Black", "White", "Red", "Blue", "Green", "Yellow", "Orange", "Purple", "Pink", "Gray",
          "Brown", "Navy", "Teal", "Maroon", "Olive", "Beige", "Charcoal", "Ivory", "Crimson",
          "Turquoise");

  static final List<String> SIZES =
      List.of(
          "XS", "S", "M", "L", "XL", "XXL", "6", "7", "8", "9", "10", "11", "12", "13", "28W30L",
          "30W32L", "32W32L", "34W34L", "36W34L", "One Size");

  /** Vocabulary for synthetic, searchable product titles. */
  static final List<String> TITLE_WORDS =
      List.of(
          "premium", "classic", "modern", "essential", "lightweight", "durable", "everyday",
          "trail", "urban", "sport", "casual", "performance", "heritage", "signature", "active",
          "outdoor", "insulated", "breathable", "slim", "relaxed", "waterproof", "reinforced",
          "packable", "quilted", "tailored", "vintage", "retro", "graphic", "solid", "striped",
          "jacket", "boot", "sneaker", "backpack", "hoodie", "jean", "sandal", "glove", "hat",
          "sock");
}
