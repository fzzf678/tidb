CREATE TABLE `users` (
    `id` int NOT NULL,
    `name` varchar(10) DEFAULT NULL,
    `email` text DEFAULT NULL,
    `age` bigint DEFAULT NULL,
    `salary` decimal(10,2) DEFAULT NULL,
    `is_active` tinyint(1) DEFAULT NULL,
    `join_date` date DEFAULT NULL,
    `last_login` timestamp NULL DEFAULT NULL,
    `role` enum('admin','user','guest') DEFAULT NULL,
    `alive` boolean DEFAULT NULL,
)
