import { BackfillAgentIdentities1794838752474 as BaseMigration } from '../common/1794838752474-BackfillAgentIdentities';

/**
 * The NOT NULL and FK changes recreate `agents` on SQLite, and several agent
 * tables have CASCADE FKs to it. Disable FKs for the migration's duration.
 */
export class BackfillAgentIdentities1794838752474 extends BaseMigration {
	withFKsDisabled = true as const;
}
