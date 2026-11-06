-- Each character's units as the last world it finished left them. Written when a
-- tutorial's flag is reached; nothing reads it yet (a resume button will).
CREATE TABLE social.character_states (
  character_id BIGINT PRIMARY KEY REFERENCES social.characters(character_id) ON DELETE CASCADE,
  units JSONB NOT NULL,
  saved_from TEXT NOT NULL,
  slot INT NOT NULL,
  generation BIGINT NOT NULL,
  saved_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
