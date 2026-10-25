import { Type } from "typebox";

/** Static card type defaults. contracts.ts applies the configured runtime goal limit. */
export const TASK_CARD_GOAL_MAX_LENGTH = 1_600;
export const TASK_ID_MAX_LENGTH = 128;
export const TASK_CARD_TITLE_MAX_LENGTH = 80;
export const TASK_CARD_CONTEXT_MAX_LENGTH = 2_000;
export const TaskVersionRefSchema = Type.String({ pattern: "^v_[0-9a-f]{16}$", minLength: 18, maxLength: 18 });
