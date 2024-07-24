class ZymoProduct:
    ID_PREFIX = "ZYMO_PRODUCT:"
    LABEL = "zymo_product"

    class PropertyKey:
        UUID = "uuid"  # same as NAME
        NAME = "name"  # same as SKU
        CATEGORY = "category"
        PRODUCT_CLASS = "product_class"
        ITEM_TYPE = "item_type"
        DESCRIPTION = "description"
        SHORT_DESCRIPTION = "short_description"
        BASE_PRICE = "base_price"
        SKU = "sku"
        INACTIVE = "inactive"
        SHELF_LIFE = "shelf_life"
        STORAGE_TEMPERATURE = "storage_temperature"
        SHIPPING_TEMPERATURE = "shipping_temperature"
        FEATURES = "features"
        LENGTH = "length"
        WIDTH = "width"
        HEIGHT = "height"
        WEIGHT = "weight"
        AVAILABLE_STOCK = "available_stock"
        SAFETY_INFORMATION = "safety_information"
        PRODUCT_URL = "product_url"
        IMAGE_URL = "image_url"
