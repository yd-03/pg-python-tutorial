CREATE TABLE IF NOT EXISTS happiness (
    survey_yr INTEGER NOT NULL,
    country VARCHAR(24) NOT NULL,
    overall_rank INTEGER NOT NULL,
    score DECIMAL(15, 14) NOT NULL,
    gdp DECIMAL(17, 16) NOT NULL,
    generosity DECIMAL(16, 16) NOT NULL,
    freedom DECIMAL(16, 16) NOT NULL,
    social_support DECIMAL(16, 15) NOT NULL,
    life_exp DECIMAL(17, 16) NOT NULL,
    gov_trust DECIMAL(16, 16) NOT NULL
);
