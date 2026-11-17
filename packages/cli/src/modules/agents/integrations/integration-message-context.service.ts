import type { ToolContext } from '@n8n/agents';
import { Logger } from '@n8n/backend-common';
import { Service } from '@n8n/di';
import { isRecord } from '@n8n/utils/is-record';
import { jsonParse } from 'n8n-workflow';

import type {
	IntegrationMessageContext,
	IntegrationMessageContextStore,
	SessionBinding,
} from './integration-tool-types';
import {
	isIntegrationMessageContext,
	readIntegrationMessageContext,
} from './integration-message-context';
import { AgentResourceRepository } from '../repositories/agent-resource.repository';
import { AgentThreadRepository } from '../repositories/agent-thread.repository';

const MESSAGE_CONTEXT_METADATA_KEY = 'currentMessageContext';
const CONTINUE_AS_METADATA_KEY = 'continueAs';
const BOUND_THREADS_METADATA_KEY = 'boundThreads';
const SENT_MESSAGES_METADATA_KEY = 'sentMessages';

/** Older entries drop out, so the metadata of a busy thread stays small. */
const MAX_SENT_MESSAGES_PER_THREAD = 50;

@Service()
export class IntegrationMessageContextService implements IntegrationMessageContextStore {
	constructor(
		private readonly threadRepository: AgentThreadRepository,
		private readonly resourceRepository: AgentResourceRepository,
		private readonly logger: Logger,
	) {}

	async getLatest(threadId: string): Promise<IntegrationMessageContext | null> {
		const thread = await this.threadRepository.findOneBy({ id: threadId });
		const value = this.parseMetadata(thread?.metadata)[MESSAGE_CONTEXT_METADATA_KEY];
		return isIntegrationMessageContext(value) ? value : null;
	}

	async getForResume(
		persistence: NonNullable<ToolContext['persistence']>,
	): Promise<IntegrationMessageContext | null> {
		const context = readIntegrationMessageContext(persistence);
		return context === undefined ? await this.getLatest(persistence.threadId) : context;
	}

	async getLatestForIncoming(threadId: string): Promise<IntegrationMessageContext | null> {
		try {
			return await this.getLatest(threadId);
		} catch (error) {
			this.logger.warn('Failed to read previous integration message context', {
				threadId,
				error: error instanceof Error ? error.message : String(error),
			});
			return null;
		}
	}

	async installIncoming(
		context: IntegrationMessageContext,
		memory: SessionBinding,
		conversation: SessionBinding,
	): Promise<void> {
		const scopes =
			conversation.threadId === memory.threadId ? [conversation] : [conversation, memory];
		for (const { threadId, resourceId } of scopes) {
			try {
				await this.setLatest(threadId, resourceId, context);
			} catch (error) {
				this.logger.warn('Failed to update integration message context', {
					threadId,
					error: error instanceof Error ? error.message : String(error),
				});
			}
		}
	}

	async setLatest(
		threadId: string,
		resourceId: string,
		context: IntegrationMessageContext,
	): Promise<void> {
		await this.writeMetadata(threadId, resourceId, {
			[MESSAGE_CONTEXT_METADATA_KEY]: context,
		});
	}

	async bindSession(derivedThreadId: string, origin: SessionBinding): Promise<void> {
		if (derivedThreadId === origin.threadId) return;
		if (await this.resolveSession(derivedThreadId)) return;

		await this.appendBoundThread(origin.threadId, derivedThreadId);
		await this.writeContinueAsIfAbsent(derivedThreadId, origin);
	}

	async resolveSession(derivedThreadId: string): Promise<SessionBinding | null> {
		const thread = await this.threadRepository.findOneBy({ id: derivedThreadId });
		const value = this.parseMetadata(thread?.metadata)[CONTINUE_AS_METADATA_KEY];
		return isSessionBinding(value) ? value : null;
	}

	async recordSentMessage(
		derivedThreadId: string,
		messageId: string,
		origin: SessionBinding,
	): Promise<void> {
		await this.writeMetadata(derivedThreadId, origin.resourceId, (metadata) => {
			const previous = metadata[SENT_MESSAGES_METADATA_KEY];
			const entries = isRecord(previous) ? Object.entries(previous) : [];
			return {
				[SENT_MESSAGES_METADATA_KEY]: Object.fromEntries(
					[...entries.filter(([id]) => id !== messageId), [messageId, origin]].slice(
						-MAX_SENT_MESSAGES_PER_THREAD,
					),
				),
			};
		});
	}

	async resolveSentMessage(
		derivedThreadId: string,
		messageId: string,
	): Promise<SessionBinding | null> {
		const thread = await this.threadRepository.findOneBy({ id: derivedThreadId });
		const sentMessages = this.parseMetadata(thread?.metadata)[SENT_MESSAGES_METADATA_KEY];
		const value = isRecord(sentMessages) ? sentMessages[messageId] : undefined;
		return isSessionBinding(value) ? value : null;
	}

	async unbindSession(derivedThreadId: string): Promise<void> {
		await this.writeMetadata(derivedThreadId, undefined, {
			[CONTINUE_AS_METADATA_KEY]: undefined,
		});
	}

	async clearSessionBindings(originThreadId: string): Promise<void> {
		const existing = await this.threadRepository.findOneBy({ id: originThreadId });
		const metadata = this.parseMetadata(existing?.metadata);
		const bound = Array.isArray(metadata[BOUND_THREADS_METADATA_KEY])
			? (metadata[BOUND_THREADS_METADATA_KEY] as string[])
			: [];
		if (bound.length === 0) return;
		for (const id of bound) await this.unbindSession(id);
		await this.writeMetadata(originThreadId, undefined, { [BOUND_THREADS_METADATA_KEY]: [] });
	}

	private async writeContinueAsIfAbsent(
		derivedThreadId: string,
		origin: SessionBinding,
	): Promise<void> {
		await this.writeMetadata(derivedThreadId, origin.resourceId, (metadata) =>
			isSessionBinding(metadata[CONTINUE_AS_METADATA_KEY])
				? null
				: { [CONTINUE_AS_METADATA_KEY]: origin },
		);
	}

	private async appendBoundThread(originThreadId: string, derivedThreadId: string): Promise<void> {
		await this.writeMetadata(originThreadId, originThreadId, (metadata) => {
			const previous = metadata[BOUND_THREADS_METADATA_KEY];
			const bound = Array.isArray(previous) ? previous.filter((id) => id !== derivedThreadId) : [];
			return { [BOUND_THREADS_METADATA_KEY]: [...bound, derivedThreadId] };
		});
	}

	private async writeMetadata(
		threadId: string,
		resourceId: string | undefined,
		patch:
			| Record<string, unknown>
			| ((metadata: Record<string, unknown>) => Record<string, unknown> | null),
	): Promise<void> {
		const merge = (value: string | null | undefined) => {
			const metadata = this.parseMetadata(value);
			const update = typeof patch === 'function' ? patch(metadata) : patch;
			return update ? JSON.stringify({ ...metadata, ...update }) : null;
		};
		const write = async () =>
			await this.threadRepository.patchThread(
				threadId,
				(row) => {
					const metadata = merge(row.metadata);
					return metadata === null ? null : { metadata };
				},
				{},
			);
		if (await write()) return;
		if (merge(null) === null) return;
		await this.resourceRepository.ensureExists(resourceId ?? threadId);
		await this.threadRepository.ensureExists(threadId, resourceId ?? threadId);
		await write();
	}

	private parseMetadata(value: string | null | undefined): Record<string, unknown> {
		if (!value) return {};
		try {
			const parsed = jsonParse<unknown>(value);
			return isRecord(parsed) ? parsed : {};
		} catch {
			return {};
		}
	}
}

function isSessionBinding(value: unknown): value is SessionBinding {
	return (
		isRecord(value) && typeof value.threadId === 'string' && typeof value.resourceId === 'string'
	);
}
