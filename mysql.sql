CREATE TABLE `url_mapping`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `short_id` varchar(44) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `path` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `origin_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `create_time` datetime(0) NOT NULL,
  `last_update_time` datetime(0) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `path_unq`(`path`) USING BTREE,
  UNIQUE INDEX `short_id_unq`(`short_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;
