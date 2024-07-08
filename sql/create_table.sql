CREATE TABLE IF NOT EXISTS happiness (
    survey_yr INTEGER NOT NULL,
    country VARCHAR(24) NOT NULL,
    overall_rank INTEGER NOT NULL,
    score DECIMAL(10, 4) NOT NULL,
    gdp DECIMAL(10, 5) NOT NULL,
    generosity DECIMAL(10, 5) NOT NULL,
    freedom DECIMAL(10, 5) NOT NULL,
    social_support DECIMAL(10, 5) NOT NULL,
    life_exp DECIMAL(10, 5) NOT NULL,
    gov_trust DECIMAL(10, 5) NOT NULL
);
