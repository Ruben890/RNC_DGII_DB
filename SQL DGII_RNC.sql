CREATE TABLE RNC (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rnc VARCHAR(11) NOT NULL UNIQUE,
    nombre_apellido VARCHAR(100) NOT NULL,
    actividad_economica VARCHAR(255),
    fecha DATE,
    estado  VARCHAR(50) NOT NULL,
    tipo_contribuyente VARCHAR(50) NOT NULL
);
