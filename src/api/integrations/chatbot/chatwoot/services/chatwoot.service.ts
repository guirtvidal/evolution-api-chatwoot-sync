import { InstanceDto } from '@api/dto/instance.dto';
import { Options, Quoted, SendAudioDto, SendMediaDto, SendTextDto } from '@api/dto/sendMessage.dto';
import { ChatwootDto } from '@api/integrations/chatbot/chatwoot/dto/chatwoot.dto';
import { postgresClient } from '@api/integrations/chatbot/chatwoot/libs/postgres.client';
import { chatwootImport } from '@api/integrations/chatbot/chatwoot/utils/chatwoot-import-helper';
import { PrismaRepository } from '@api/repository/repository.service';
import { CacheService } from '@api/services/cache.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { Events } from '@api/types/wa.types';
import { Chatwoot, ConfigService, Database, HttpServer } from '@config/env.config';
import { Logger } from '@config/logger.config';
import ChatwootClient, {
  ChatwootAPIConfig,
  contact,
  contact_inboxes,
  conversation,
  conversation_show,
  generic_id,
  inbox,
} from '@figuro/chatwoot-sdk';
import { request as chatwootRequest } from '@figuro/chatwoot-sdk/dist/core/request';
import { Chatwoot as ChatwootModel, Contact as ContactModel, Message as MessageModel } from '@prisma/client';
import i18next from '@utils/i18n';
import { sendTelemetry } from '@utils/sendTelemetry';
import { AsyncLocalStorage } from 'async_hooks';
import axios from 'axios';
import { WAMessageContent, WAMessageKey } from 'baileys';
import dayjs from 'dayjs';
import FormData from 'form-data';
import { Jimp, JimpMime } from 'jimp';
import { parsePhoneNumberFromString } from 'libphonenumber-js';
import Long from 'long';
import mimeTypes from 'mime-types';
import path from 'path';
import { Readable } from 'stream';

interface ChatwootMessage {
  messageId?: number;
  inboxId?: number;
  conversationId?: number;
  contactInboxSourceId?: string;
  isRead?: boolean;
}

interface ChatwootMessageReference {
  chatwootMessageId: number;
  chatwootConversationId: number;
  chatwootInboxId?: number;
  chatwootContactInboxSourceId?: string;
}

export class ChatwootService {
  private readonly logger = new Logger('ChatwootService');

  // Lock polling delay
  private readonly LOCK_POLLING_DELAY_MS = 300; // Delay between lock status checks
  private readonly IMPORT_HISTORY_IDLE_DELAY_MS = 45_000;
  private readonly IMPORT_HISTORY_START_DELAY_MS = 120_000;
  private readonly importHistoryTimers = new Map<string, NodeJS.Timeout>();
  private readonly importHistoryRunning = new Set<string>();
  private readonly outgoingDeliveriesInProgress = new Set<string>();
  private readonly identityReconciliations = new Map<string, Promise<any | null>>();

  private readonly providerContext = new AsyncLocalStorage<any>();
  private fallbackProvider: any;

  private get provider(): any {
    return this.providerContext.getStore() || this.fallbackProvider;
  }

  private set provider(provider: any) {
    this.fallbackProvider = provider;
    this.providerContext.enterWith(provider);
  }

  constructor(
    private readonly waMonitor: WAMonitoringService,
    private readonly configService: ConfigService,
    private readonly prismaRepository: PrismaRepository,
    private readonly cache: CacheService,
  ) {}

  private pgClient = postgresClient.getChatwootConnection();

  private formatError(error: unknown): string {
    if (error instanceof Error) {
      const requestError = error as Error & {
        body?: unknown;
        response?: { data?: unknown; status?: number };
        status?: number;
        statusText?: string;
      };
      const responseDetails = {
        status: requestError.status || requestError.response?.status,
        statusText: requestError.statusText,
        body: requestError.body || requestError.response?.data,
      };
      const hasResponseDetails = Object.values(responseDetails).some((value) => value !== undefined);

      if (hasResponseDetails) {
        try {
          return `${error.stack || error.message} response=${JSON.stringify(responseDetails)}`;
        } catch {
          // Fall back to the stack if the HTTP client returned a circular response body.
        }
      }

      return error.stack || error.message;
    }

    try {
      return JSON.stringify(error) ?? String(error);
    } catch {
      return String(error);
    }
  }

  private async beginOutgoingDelivery(deliveryKey: string): Promise<boolean> {
    if (this.outgoingDeliveriesInProgress.has(deliveryKey) || (await this.cache.has(`${deliveryKey}:sent`))) {
      return false;
    }

    const deliveryReserved = await this.cache.setIfNotExists(`${deliveryKey}:processing`, true, 2 * 60);
    if (!deliveryReserved) {
      return false;
    }

    this.outgoingDeliveriesInProgress.add(deliveryKey);
    return true;
  }

  private async finishOutgoingDelivery(deliveryKey: string, sent: boolean) {
    if (sent) {
      await this.cache.set(`${deliveryKey}:sent`, true, 24 * 60 * 60);
    }
    await this.cache.delete(`${deliveryKey}:processing`);
    this.outgoingDeliveriesInProgress.delete(deliveryKey);
  }

  private async getProvider(instance: InstanceDto): Promise<ChatwootModel | null> {
    const cacheKey = `${instance.instanceName}:getProvider`;
    if (await this.cache.has(cacheKey)) {
      const provider = (await this.cache.get(cacheKey)) as ChatwootModel;

      return provider;
    }

    const provider = await this.waMonitor.waInstances[instance.instanceName]?.findChatwoot();

    if (!provider) {
      this.logger.warn('provider not found');
      return null;
    }

    await this.cache.set(cacheKey, provider, 5 * 60);

    return provider;
  }

  private async clientCw(instance: InstanceDto) {
    const provider = await this.getProvider(instance);

    if (!provider) {
      this.logger.error('provider not found');
      return null;
    }

    this.provider = provider;

    const client = new ChatwootClient({
      config: this.getClientCwConfig(),
    });

    return client;
  }

  public getClientCwConfig(): ChatwootAPIConfig & { nameInbox: string; mergeBrazilContacts: boolean } {
    return {
      basePath: this.provider.url,
      with_credentials: true,
      credentials: 'include',
      token: this.provider.token,
      nameInbox: this.provider.nameInbox,
      mergeBrazilContacts: this.provider.mergeBrazilContacts,
    };
  }

  public getCache() {
    return this.cache;
  }

  public async create(instance: InstanceDto, data: ChatwootDto) {
    await this.waMonitor.waInstances[instance.instanceName].setChatwoot(data);
    await this.cache.delete(`${instance.instanceName}:getProvider`);

    if (data.autoCreate) {
      this.logger.log('Auto create chatwoot instance');
      const urlServer = this.configService.get<HttpServer>('SERVER').URL;

      await this.initInstanceChatwoot(
        instance,
        data.nameInbox ?? instance.instanceName.split('-cwId-')[0],
        `${urlServer}/chatwoot/webhook/${encodeURIComponent(instance.instanceName)}`,
        true,
        data.number,
        data.organization,
        data.logo,
      );
    }
    return data;
  }

  public async find(instance: InstanceDto): Promise<ChatwootDto> {
    try {
      return await this.waMonitor.waInstances[instance.instanceName].findChatwoot();
    } catch {
      this.logger.error('chatwoot not found');
      return { enabled: null, url: '' };
    }
  }

  public async getContact(instance: InstanceDto, id: number) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    if (!id) {
      this.logger.warn('id is required');
      return null;
    }

    const contact = await client.contact.getContactable({
      accountId: this.provider.accountId,
      id,
    });

    if (!contact) {
      this.logger.warn('contact not found');
      return null;
    }

    return contact;
  }

  public async initInstanceChatwoot(
    instance: InstanceDto,
    inboxName: string,
    webhookUrl: string,
    qrcode: boolean,
    number: string,
    organization?: string,
    logo?: string,
  ) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const findInbox: any = await client.inboxes.list({
      accountId: this.provider.accountId,
    });

    const checkDuplicate = findInbox.payload.map((inbox) => inbox.name).includes(inboxName);

    let inboxId: number;

    this.logger.log('Creating chatwoot inbox');
    if (!checkDuplicate) {
      const data = {
        type: 'api',
        webhook_url: webhookUrl,
      };

      const inbox = await client.inboxes.create({
        accountId: this.provider.accountId,
        data: {
          name: inboxName,
          channel: data as any,
        },
      });

      if (!inbox) {
        this.logger.warn('inbox not found');
        return null;
      }

      inboxId = inbox.id;
    } else {
      const inbox = findInbox.payload.find((inbox) => inbox.name === inboxName);

      if (!inbox) {
        this.logger.warn('inbox not found');
        return null;
      }

      inboxId = inbox.id;
    }
    this.logger.log(`Inbox created - inboxId: ${inboxId}`);

    if (!this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT) {
      this.logger.log('Chatwoot bot contact is disabled');

      return true;
    }

    this.logger.log('Creating chatwoot bot contact');
    const contact =
      (await this.findContact(instance, '123456')) ||
      ((await this.createContact(
        instance,
        '123456',
        inboxId,
        false,
        organization ? organization : 'EvolutionAPI',
        logo ? logo : 'https://evolution-api.com/files/evolution-api-favicon.png',
      )) as any);

    if (!contact) {
      this.logger.warn('contact not found');
      return null;
    }

    const contactId = contact.id || contact.payload.contact.id;
    this.logger.log(`Contact created - contactId: ${contactId}`);

    if (qrcode) {
      this.logger.log('QR code enabled');
      const data = {
        contact_id: contactId.toString(),
        inbox_id: inboxId.toString(),
      };

      const conversation = await client.conversations.create({
        accountId: this.provider.accountId,
        data,
      });

      if (!conversation) {
        this.logger.warn('conversation not found');
        return null;
      }

      let contentMsg = 'init';

      if (number) {
        contentMsg = `init:${number}`;
      }

      const message = await client.messages.create({
        accountId: this.provider.accountId,
        conversationId: conversation.id,
        data: {
          content: contentMsg,
          message_type: 'outgoing',
        },
      });

      if (!message) {
        this.logger.warn('conversation not found');
        return null;
      }
      this.logger.log('Init message sent');
    }

    return true;
  }

  public async createContact(
    instance: InstanceDto,
    phoneNumber: string,
    inboxId: number,
    isGroup: boolean,
    name?: string,
    avatar_url?: string,
    jid?: string,
  ) {
    try {
      const client = await this.clientCw(instance);

      if (!client) {
        this.logger.warn('client not found');
        return null;
      }

      let data: any = {};
      if (!isGroup) {
        const chatwootPhoneNumber = this.getChatwootPhoneNumber(phoneNumber, jid);
        const contactName = this.isUsableContactName(name)
          ? name.trim()
          : chatwootPhoneNumber
            ? `Contato WhatsApp ${chatwootPhoneNumber}`
            : 'Contato WhatsApp';

        data = {
          inbox_id: inboxId,
          name: contactName,
          identifier: jid,
          avatar_url: avatar_url,
        };

        if (chatwootPhoneNumber) {
          data['phone_number'] = chatwootPhoneNumber;
        }
      } else {
        data = {
          inbox_id: inboxId,
          name: name || phoneNumber,
          identifier: phoneNumber,
          avatar_url: avatar_url,
        };
      }

      const contact = await client.contacts.create({
        accountId: this.provider.accountId,
        data,
      });

      if (!contact) {
        this.logger.warn('contact not found');
        return null;
      }

      const contactPayload = contact as any;
      const contactId = contactPayload?.payload?.id || contactPayload?.payload?.contact?.id || contactPayload?.id;
      if (contactId) {
        await this.addLabelToContact(this.provider.nameInbox, contactId);
      }

      return contact;
    } catch (error) {
      if ((error.status === 422 || error.response?.status === 422) && jid) {
        this.logger.warn(`Contact creation failed (422) for ${jid}. Reconciling existing Chatwoot identities...`);
        const chatwootPhoneNumber = this.getChatwootPhoneNumber(phoneNumber, jid);
        const normalizedPhoneJid = chatwootPhoneNumber
          ? `${chatwootPhoneNumber.replace(/\D/g, '')}@s.whatsapp.net`
          : null;
        const existingContact =
          !isGroup && normalizedPhoneJid
            ? await this.reconcileChatwootContactIdentity(
                instance,
                normalizedPhoneJid,
                jid.includes('@lid') ? jid : null,
                name,
              )
            : await this.findContactByIdentifier(instance, jid);
        if (existingContact) {
          const contactId = existingContact.id;

          await this.addLabelToContact(this.provider.nameInbox, contactId);
          return existingContact;
        }
      }

      this.logger.error(`Error creating Chatwoot contact: ${this.formatError(error)}`);
      return null;
    }
  }

  private getChatwootPhoneNumber(phoneNumber?: string, jid?: string): string | null {
    const candidates = [phoneNumber, jid];

    for (const candidate of candidates) {
      if (!candidate || candidate.includes('@lid') || candidate.includes('@g.us')) {
        continue;
      }

      const number = candidate.split('@')[0].split(':')[0].replace(/\D/g, '');

      if (number.length >= 8 && number.length <= 15) {
        return `+${number}`;
      }
    }

    return null;
  }

  private isUsableContactName(name?: string | null): boolean {
    if (!name?.trim()) {
      return false;
    }

    const normalizedName = name.trim();
    const normalizedPlaceholder = normalizedName
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase();

    return (
      !['voce', 'you', 'me', 'eu', 'myself'].includes(normalizedPlaceholder) &&
      !normalizedName.includes('@s.whatsapp.net') &&
      !normalizedName.includes('@lid') &&
      !/^\+?[\d\s().:-]+$/.test(normalizedName)
    );
  }

  private isGeneratedContactName(name?: string | null): boolean {
    if (!name?.trim()) {
      return false;
    }

    const normalizedName = name
      .trim()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase();

    return normalizedName.startsWith('contato whatsapp') || normalizedName.startsWith('whatsapp contact');
  }

  private async resolveContactName(instance: InstanceDto, body: any, phoneNumber: string): Promise<string | null> {
    if (!body.key?.fromMe && this.isUsableContactName(body.pushName)) {
      return body.pushName.trim();
    }

    const remoteJids = [body.key?.remoteJid, body.key?.remoteJidAlt, body.key?.remoteJidLid, phoneNumber].filter(
      (jid, index, values) => typeof jid === 'string' && values.indexOf(jid) === index,
    );

    const storedContact = await this.prismaRepository.contact.findFirst({
      where: {
        instanceId: instance.instanceId,
        remoteJid: { in: remoteJids },
      },
      orderBy: { updatedAt: 'desc' },
    });

    return this.isUsableContactName(storedContact?.pushName) ? storedContact.pushName.trim() : null;
  }

  public async updateContact(instance: InstanceDto, id: number, data: any) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    if (!id) {
      this.logger.warn('id is required');
      return null;
    }

    try {
      const contact = (await client.contacts.update({
        accountId: this.provider.accountId,
        id,
        data,
      })) as any;

      return contact || { id, ...data };
    } catch (error) {
      const errorDetails = {
        message: error?.message,
        status: error?.status || error?.response?.status,
        statusText: error?.statusText,
        body: error?.body || error?.response?.data,
      };
      this.logger.error(
        `Error updating Chatwoot contact ${id} with data=${JSON.stringify(data)}: ${JSON.stringify(errorDetails)}`,
      );
      return null;
    }
  }

  public async addLabelToContact(nameInbox: string, contactId: number) {
    try {
      const uri = this.configService.get<Chatwoot>('CHATWOOT').IMPORT.DATABASE.CONNECTION.URI;

      if (!uri || !contactId || !nameInbox) return false;

      const sqlTag = `INSERT INTO tags (name, taggings_count)
                      VALUES ($1, 0)
                      ON CONFLICT (name)
                      DO UPDATE SET name = EXCLUDED.name
                      RETURNING id`;

      const tagId = (await this.pgClient.query(sqlTag, [nameInbox]))?.rows[0]?.id;
      if (!tagId) return false;

      const sqlCheckTagging = `SELECT 1 FROM taggings 
                               WHERE tag_id = $1 AND taggable_type = 'Contact' AND taggable_id = $2 AND context = 'labels' LIMIT 1`;

      const taggingExists = (await this.pgClient.query(sqlCheckTagging, [tagId, contactId]))?.rowCount > 0;

      if (!taggingExists) {
        const sqlInsertLabel = `INSERT INTO taggings (tag_id, taggable_type, taggable_id, context, created_at) 
                                VALUES ($1, 'Contact', $2, 'labels', NOW())`;

        await this.pgClient.query(sqlInsertLabel, [tagId, contactId]);
        await this.pgClient.query(`UPDATE tags SET taggings_count = taggings_count + 1 WHERE id = $1`, [tagId]);
      }

      return true;
    } catch {
      return false;
    }
  }

  public async findContactByIdentifier(instance: InstanceDto, identifier: string) {
    if (!(await this.clientCw(instance))) {
      this.logger.warn('client not found');
      return null;
    }

    try {
      try {
        const databaseContacts = await this.findContactsByIdentifierInChatwootDb(identifier);

        if (databaseContacts[0]) {
          return databaseContacts[0];
        }
      } catch (error) {
        this.logger.warn(
          `Unable to find Chatwoot contact by identifier in database ${identifier}: ${error?.toString?.() || error}`,
        );
      }

      const contact = await chatwootRequest<any>(this.getClientCwConfig(), {
        method: 'GET',
        url: `/api/v1/accounts/${this.provider.accountId}/contacts/search`,
        query: {
          q: identifier,
          sort: 'name',
        },
      });
      const searchPayload = contact?.payload || contact?.data?.payload || [];
      const exactContact = searchPayload.find((item) => item.identifier === identifier);

      if (exactContact) {
        return exactContact;
      }

      const contactByAttr = await chatwootRequest<any>(this.getClientCwConfig(), {
        method: 'POST',
        url: `/api/v1/accounts/${this.provider.accountId}/contacts/filter`,
        body: {
          payload: [
            {
              attribute_key: 'identifier',
              filter_operator: 'equal_to',
              values: [identifier],
              query_operator: null,
            },
          ],
        },
      });
      const filterPayload = contactByAttr?.payload || contactByAttr?.data?.payload || [];

      return filterPayload.find((item) => item.identifier === identifier) || null;
    } catch (error) {
      this.logger.warn(`Unable to find Chatwoot contact by identifier ${identifier}: ${error?.toString?.() || error}`);
      return null;
    }
  }

  private async findContactsByIdentifierInChatwootDb(identifier: string): Promise<any[]> {
    const databaseContacts = await this.pgClient.query(
      `SELECT id, name, phone_number, identifier
       FROM contacts
       WHERE account_id = $1
         AND identifier = $2
       ORDER BY updated_at DESC NULLS LAST, id DESC`,
      [this.provider.accountId, identifier],
    );

    return databaseContacts?.rows || [];
  }

  private async findChatwootContactIdentityCandidates(
    instance: InstanceDto,
    phoneJid: string,
    lidJid: string | null,
  ): Promise<any[]> {
    const phoneNumber = phoneJid.split('@')[0].split(':')[0].replace(/\D/g, '');
    const chatwootPhoneNumber = this.getChatwootPhoneNumber(phoneJid);
    const phoneVariants = chatwootPhoneNumber
      ? chatwootPhoneNumber.startsWith('+55') && this.getClientCwConfig().mergeBrazilContacts
        ? this.getNumbers(chatwootPhoneNumber)
        : [chatwootPhoneNumber]
      : [];
    const lidNumber = lidJid?.split('@')[0].split(':')[0].replace(/\D/g, '');
    const phoneDigits = Array.from(
      new Set(phoneVariants.map((number) => number.replace(/\D/g, '')).filter((number) => !!number)),
    );
    const legacyLidIdentifiers = lidNumber
      ? [lidNumber, `+${lidNumber}`, `${lidNumber}@s.whatsapp.net`, `${lidNumber}@lid`]
      : [];
    const identifiers = Array.from(
      new Set(
        [phoneJid, lidJid, phoneNumber, chatwootPhoneNumber, ...legacyLidIdentifiers].filter(
          (identifier): identifier is string => !!identifier,
        ),
      ),
    );
    const candidates = new Map<number, any>();
    const addCandidates = (contacts: any[]) => {
      for (const contact of contacts || []) {
        const contactId = Number(contact?.id);
        if (!Number.isFinite(contactId) || candidates.has(contactId)) {
          continue;
        }
        candidates.set(contactId, { ...contact, id: contactId });
      }
    };

    try {
      const databaseContacts = await this.pgClient.query(
        `SELECT
           contacts.id,
           contacts.name,
           contacts.email,
           contacts.phone_number,
           contacts.identifier,
           contacts.created_at,
           contacts.updated_at,
           COUNT(DISTINCT conversations.id)::int AS conversation_count
         FROM contacts
         LEFT JOIN conversations
           ON conversations.account_id = contacts.account_id
          AND conversations.contact_id = contacts.id
         WHERE contacts.account_id = $1
           AND (
             contacts.identifier = ANY($2::text[])
             OR regexp_replace(COALESCE(contacts.phone_number, ''), '[^0-9]', '', 'g') = ANY($3::text[])
           )
         GROUP BY
           contacts.id,
           contacts.name,
           contacts.email,
           contacts.phone_number,
           contacts.identifier,
           contacts.created_at,
           contacts.updated_at
         ORDER BY contacts.created_at ASC NULLS LAST, contacts.id ASC`,
        [this.provider.accountId, identifiers, phoneDigits],
      );

      addCandidates(databaseContacts?.rows || []);
    } catch (error) {
      this.logger.warn(
        `Unable to list Chatwoot identity candidates in database phone=${phoneJid} lid=${lidJid || ''}: ${
          error?.toString?.() || error
        }`,
      );
    }

    if (phoneVariants.length > 0) {
      try {
        const contactsByPhone = await chatwootRequest<any>(this.getClientCwConfig(), {
          method: 'POST',
          url: `/api/v1/accounts/${this.provider.accountId}/contacts/filter`,
          body: {
            payload: phoneVariants.map((number, index) => ({
              attribute_key: 'phone_number',
              filter_operator: 'equal_to',
              values: [number.replace(/\D/g, '')],
              query_operator: index === phoneVariants.length - 1 ? null : 'OR',
            })),
          },
        });
        addCandidates(contactsByPhone?.payload || contactsByPhone?.data?.payload || []);
      } catch (error) {
        this.logger.warn(
          `Unable to list Chatwoot contacts by phone ${chatwootPhoneNumber}: ${error?.toString?.() || error}`,
        );
      }
    }

    for (const identifier of Array.from(new Set([lidJid, phoneJid, ...legacyLidIdentifiers]))) {
      if (!identifier) {
        continue;
      }
      const contact = await this.findContactByIdentifier(instance, identifier);
      if (contact) {
        addCandidates([contact]);
      }
    }

    return Array.from(candidates.values());
  }

  private getChatwootContactMergeScore(
    contact: any,
    chatwootPhoneNumber: string | null,
    canonicalIdentifier: string,
    preferredName?: string | null,
  ): number {
    const conversationCount = Number(contact?.conversation_count || 0);
    const safeConversationCount = Number.isFinite(conversationCount) ? Math.max(conversationCount, 0) : 0;
    let score = Math.min(safeConversationCount, 1000) * 1000;

    if (chatwootPhoneNumber && contact?.phone_number === chatwootPhoneNumber) {
      score += 300;
    }
    if (this.isUsableContactName(contact?.name)) {
      score += 200;
    }
    if (contact?.identifier === canonicalIdentifier) {
      score += 100;
    }
    if (typeof contact?.email === 'string' && contact.email.includes('@')) {
      score += 25;
    }
    if (this.isUsableContactName(preferredName) && contact?.name?.trim() === preferredName!.trim()) {
      score += 50;
    }

    return score;
  }

  private selectCanonicalChatwootContact(
    contacts: any[],
    chatwootPhoneNumber: string | null,
    canonicalIdentifier: string,
    preferredName?: string | null,
  ): any | null {
    return (
      [...contacts].sort((left, right) => {
        const scoreDifference =
          this.getChatwootContactMergeScore(right, chatwootPhoneNumber, canonicalIdentifier, preferredName) -
          this.getChatwootContactMergeScore(left, chatwootPhoneNumber, canonicalIdentifier, preferredName);

        return scoreDifference || Number(left.id) - Number(right.id);
      })[0] || null
    );
  }

  private getCanonicalChatwootContactName(
    contacts: any[],
    preferredName: string | null | undefined,
    chatwootPhoneNumber: string | null,
  ): string {
    const existingName = contacts.find(
      (contact) => this.isUsableContactName(contact?.name) && !this.isGeneratedContactName(contact?.name),
    )?.name;
    if (this.isUsableContactName(existingName)) {
      return existingName.trim();
    }

    if (this.isUsableContactName(preferredName)) {
      return preferredName!.trim();
    }

    const generatedName = contacts.find((contact) => this.isUsableContactName(contact?.name))?.name;
    if (this.isUsableContactName(generatedName)) {
      return generatedName.trim();
    }

    return chatwootPhoneNumber ? `Contato WhatsApp ${chatwootPhoneNumber}` : 'Contato WhatsApp';
  }

  private async getChatwootContactById(instance: InstanceDto, contactId: number): Promise<any | null> {
    const client = await this.clientCw(instance);
    if (!client) {
      return null;
    }

    try {
      const response = (await client.contacts.get({
        accountId: this.provider.accountId,
        id: contactId,
      })) as any;
      const contact =
        response?.payload?.contact ||
        response?.payload ||
        response?.data?.payload?.contact ||
        response?.data?.payload ||
        response?.data ||
        response;

      return contact?.id ? contact : null;
    } catch (error) {
      this.logger.warn(`Unable to refresh Chatwoot contact ${contactId}: ${error?.toString?.() || error}`);
      return null;
    }
  }

  private async clearChatwootContactUniqueIdentity(instance: InstanceDto, contact: any): Promise<boolean> {
    if (!contact?.email && !contact?.phone_number && !contact?.identifier) {
      return true;
    }

    const result = await this.updateContact(instance, contact.id, {
      ...(contact.email && { email: null }),
      ...(contact.phone_number && { phone_number: null }),
      ...(contact.identifier && { identifier: null }),
    });

    return !!result;
  }

  public async findContact(instance: InstanceDto, phoneNumber: string) {
    if (!phoneNumber || typeof phoneNumber !== 'string') {
      this.logger.warn('phoneNumber is required to find a Chatwoot contact');
      return null;
    }

    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    let query: any;
    const isGroup = phoneNumber.includes('@g.us');

    if (!isGroup) {
      query = `+${phoneNumber}`;
    } else {
      query = phoneNumber;
    }

    let contact: any;

    if (isGroup) {
      contact = await client.contacts.search({
        accountId: this.provider.accountId,
        q: query,
      });
    } else {
      contact = await chatwootRequest(this.getClientCwConfig(), {
        method: 'POST',
        url: `/api/v1/accounts/${this.provider.accountId}/contacts/filter`,
        body: {
          payload: this.getFilterPayload(query),
        },
      });
    }

    if (!contact?.payload?.length) {
      if (!isGroup) {
        const normalizedPhone = phoneNumber.split('@')[0].split(':')[0].replace('+', '');
        const contactByIdentifier = await this.findContactByIdentifier(instance, `${normalizedPhone}@s.whatsapp.net`);
        if (contactByIdentifier) {
          return contactByIdentifier;
        }
      }

      this.logger.warn('contact not found');
      return null;
    }

    if (!isGroup) {
      const contactByPhone =
        contact.payload.length > 1 ? await this.findContactInContactList(contact.payload, query) : contact.payload[0];
      if (contactByPhone) {
        return contactByPhone;
      }

      const normalizedPhone = phoneNumber.split('@')[0].split(':')[0].replace('+', '');
      const contactByIdentifier = await this.findContactByIdentifier(instance, `${normalizedPhone}@s.whatsapp.net`);
      if (contactByIdentifier) {
        return contactByIdentifier;
      }

      return null;
    } else {
      return contact.payload.find((contact) => contact.identifier === query);
    }
  }

  private async mergeContacts(baseId: number, mergeId: number): Promise<boolean> {
    try {
      await chatwootRequest<any>(this.getClientCwConfig(), {
        method: 'POST',
        url: `/api/v1/accounts/${this.provider.accountId}/actions/contact_merge`,
        body: {
          base_contact_id: baseId,
          mergee_contact_id: mergeId,
        },
      });

      return true;
    } catch (error) {
      const errorDetails = {
        message: error?.message,
        status: error?.status || error?.response?.status,
        statusText: error?.statusText,
        body: error?.body || error?.response?.data,
      };
      this.logger.error(
        `Error merging Chatwoot contacts base=${baseId}, mergee=${mergeId}: ${JSON.stringify(errorDetails)}`,
      );
      return false;
    }
  }

  private async findContactInContactList(contacts: any[], query: string) {
    const phoneNumbers = this.getNumbers(query);
    const searchableFields = this.getSearchableFields();

    const phone = phoneNumbers.reduce(
      (savedNumber, number) => (number.length > savedNumber.length ? number : savedNumber),
      '',
    );

    const contact_with9 = contacts.find((contact) => contact.phone_number === phone);
    if (contact_with9) {
      return contact_with9;
    }

    for (const contact of contacts) {
      for (const field of searchableFields) {
        if (contact[field] && phoneNumbers.includes(contact[field])) {
          return contact;
        }
      }
    }

    return null;
  }

  private getNumbers(query: string) {
    const numbers = [];
    numbers.push(query);

    if (query.startsWith('+55') && query.length === 14) {
      const withoutNine = query.slice(0, 5) + query.slice(6);
      numbers.push(withoutNine);
    } else if (query.startsWith('+55') && query.length === 13) {
      const withNine = query.slice(0, 5) + '9' + query.slice(5);
      numbers.push(withNine);
    }

    return numbers;
  }

  private getSearchableFields() {
    return ['phone_number'];
  }

  private getFilterPayload(query: string) {
    const filterPayload = [];

    const numbers = this.getNumbers(query);
    const fieldsToSearch = this.getSearchableFields();

    fieldsToSearch.forEach((field, index1) => {
      numbers.forEach((number, index2) => {
        const queryOperator = fieldsToSearch.length - 1 === index1 && numbers.length - 1 === index2 ? null : 'OR';
        filterPayload.push({
          attribute_key: field,
          filter_operator: 'equal_to',
          values: [number.replace('+', '')],
          query_operator: queryOperator,
        });
      });
    });

    return filterPayload;
  }

  private getConversationActivityTimestamp(conversation: any): number {
    const timestamp =
      conversation?.last_activity_at || conversation?.updated_at || conversation?.created_at || conversation?.id || 0;

    if (typeof timestamp === 'number') {
      return timestamp;
    }

    const parsedTimestamp = Date.parse(timestamp);
    return Number.isNaN(parsedTimestamp) ? 0 : parsedTimestamp;
  }

  private findReusableInboxConversation(conversations: any[], inboxId: number, reopenConversation: boolean) {
    const inboxConversations = conversations
      .filter((conversation) => conversation?.inbox_id == inboxId)
      .filter((conversation) => reopenConversation || conversation.status !== 'resolved')
      .sort((a, b) => this.getConversationActivityTimestamp(b) - this.getConversationActivityTimestamp(a));

    return inboxConversations[0] || null;
  }

  private async resolveContactPhoneJid(instance: InstanceDto, key: any, isGroup: boolean): Promise<string> {
    if (isGroup) {
      return key.remoteJid;
    }

    const phoneJid = [key.remoteJidAlt, key.remoteJid].find(
      (jid) => typeof jid === 'string' && jid.endsWith('@s.whatsapp.net'),
    );
    const lidJid = [key.remoteJidLid, key.remoteJid, key.remoteJidAlt].find(
      (jid) => typeof jid === 'string' && jid.includes('@lid'),
    );
    const normalizePhoneJid = (jid: string) => `${jid.split('@')[0].split(':')[0]}@s.whatsapp.net`;

    if (lidJid) {
      try {
        const waInstance = this.waMonitor.waInstances[instance.instanceName];
        const mappedPhoneJid = await waInstance?.client?.signalRepository?.lidMapping?.getPNForLID(lidJid);

        if (mappedPhoneJid) {
          return normalizePhoneJid(mappedPhoneJid);
        }
      } catch (error) {
        this.logger.warn(`Unable to resolve phone number for LID ${lidJid}: ${error?.toString?.() || error}`);
      }
    }

    return phoneJid ? normalizePhoneJid(phoneJid) : key.remoteJidAlt || key.remoteJid;
  }

  private getContactLidJid(key: any): string | null {
    return (
      [key.remoteJidLid, key.remoteJid, key.remoteJidAlt].find(
        (jid) => typeof jid === 'string' && jid.endsWith('@lid'),
      ) || null
    );
  }

  private getEmbeddedEditedMessage(body: any): { key: WAMessageKey; message: any } | null {
    let currentMessage = body?.message;

    for (let depth = 0; depth < 6 && currentMessage; depth++) {
      const protocolMessage = currentMessage.protocolMessage;
      if (protocolMessage?.editedMessage) {
        return {
          key: {
            ...body.key,
            ...(protocolMessage.key || {}),
            id: protocolMessage.key?.id || body.key?.id,
            remoteJid: protocolMessage.key?.remoteJid || body.key?.remoteJid,
          },
          message: protocolMessage.editedMessage,
        };
      }

      const editedMessage = currentMessage.editedMessage?.message;
      if (editedMessage) {
        if (editedMessage.protocolMessage) {
          currentMessage = editedMessage;
          continue;
        }

        return {
          key: body.key,
          message: editedMessage,
        };
      }

      currentMessage =
        currentMessage.ephemeralMessage?.message ||
        currentMessage.viewOnceMessage?.message ||
        currentMessage.viewOnceMessageV2?.message ||
        currentMessage.viewOnceMessageV2Extension?.message ||
        currentMessage.documentWithCaptionMessage?.message;
    }

    return null;
  }

  private isEditedMessageEnvelope(body: any): boolean {
    const messageType = typeof body?.messageType === 'string' ? body.messageType.toLowerCase() : '';

    return (
      body?.status === 'EDITED' ||
      messageType === 'editedmessage' ||
      messageType === 'protocolmessage' ||
      !!body?.editedMessage ||
      !!this.getEmbeddedEditedMessage(body)
    );
  }

  private async reconcileChatwootContactIdentity(
    instance: InstanceDto,
    phoneJid: string,
    lidJid: string | null,
    preferredName?: string | null,
  ): Promise<any | null> {
    const reconciliationKey = `${instance.instanceName}:${phoneJid}`;
    const runningReconciliation = this.identityReconciliations.get(reconciliationKey);
    if (runningReconciliation) {
      return await runningReconciliation;
    }

    const reconciliation = (async () => {
      const lockKey = `${instance.instanceName}:lock:contactIdentity-${phoneJid}`;
      const lockAcquired = await this.cache.setIfNotExists(lockKey, true, 60);

      if (!lockAcquired) {
        const waitStartedAt = Date.now();
        while ((await this.cache.has(lockKey)) && Date.now() - waitStartedAt < 10_000) {
          await new Promise((resolve) => setTimeout(resolve, this.LOCK_POLLING_DELAY_MS));
        }

        const candidates = await this.findChatwootContactIdentityCandidates(instance, phoneJid, lidJid);
        return this.selectCanonicalChatwootContact(
          candidates,
          this.getChatwootPhoneNumber(phoneJid),
          lidJid || phoneJid,
          preferredName,
        );
      }

      try {
        return await this.reconcileChatwootContactIdentityInternal(instance, phoneJid, lidJid, preferredName);
      } finally {
        await this.cache.delete(lockKey);
      }
    })();
    this.identityReconciliations.set(reconciliationKey, reconciliation);

    try {
      return await reconciliation;
    } finally {
      this.identityReconciliations.delete(reconciliationKey);
    }
  }

  private async reconcileChatwootContactIdentityInternal(
    instance: InstanceDto,
    phoneJid: string,
    lidJid: string | null,
    preferredName?: string | null,
  ): Promise<any | null> {
    const chatwootPhoneNumber = this.getChatwootPhoneNumber(phoneJid);
    const canonicalIdentifier = lidJid || phoneJid;
    const candidates = await this.findChatwootContactIdentityCandidates(instance, phoneJid, lidJid);
    const canonicalContact = this.selectCanonicalChatwootContact(
      candidates,
      chatwootPhoneNumber,
      canonicalIdentifier,
      preferredName,
    );

    if (!canonicalContact) {
      return null;
    }

    const canonicalName = this.getCanonicalChatwootContactName(
      [canonicalContact, ...candidates.filter((contact) => contact.id !== canonicalContact.id)],
      preferredName,
      chatwootPhoneNumber,
    );
    const mergeeContacts = candidates.filter((contact) => contact.id !== canonicalContact.id);
    const canonicalEmail =
      canonicalContact.email ||
      candidates.find((contact) => typeof contact?.email === 'string' && contact.email)?.email;
    const canonicalIdentitySnapshot = {
      email: canonicalContact.email || null,
      phone_number: canonicalContact.phone_number || null,
      identifier: canonicalContact.identifier || null,
    };

    this.logger.info(
      `[CW.CONTACT] Reconciliation phone=${chatwootPhoneNumber || 'unresolved'} lid=${
        lidJid || 'none'
      } canonical=${canonicalContact.id} candidates=${JSON.stringify(
        candidates.map((contact) => ({
          id: contact.id,
          name: contact.name,
          phone_number: contact.phone_number,
          identifier: contact.identifier,
          conversations: Number(contact.conversation_count || 0),
        })),
      )}`,
    );

    if (mergeeContacts.length > 0) {
      const canonicalCleared = await this.clearChatwootContactUniqueIdentity(instance, canonicalContact);
      if (!canonicalCleared) {
        this.logger.error(`[CW.CONTACT] Unable to prepare canonical contact ${canonicalContact.id} for merge`);
        return canonicalContact;
      }

      for (const mergeeContact of mergeeContacts) {
        const mergeeCleared = await this.clearChatwootContactUniqueIdentity(instance, mergeeContact);
        if (!mergeeCleared) {
          this.logger.error(`[CW.CONTACT] Unable to prepare duplicate contact ${mergeeContact.id} for merge`);
          await this.updateContact(instance, canonicalContact.id, canonicalIdentitySnapshot);
          return (await this.getChatwootContactById(instance, canonicalContact.id)) || canonicalContact;
        }

        this.logger.warn(
          `[CW.CONTACT] Merging duplicate contact base=${canonicalContact.id} mergee=${mergeeContact.id}`,
        );
        const merged = await this.mergeContacts(canonicalContact.id, mergeeContact.id);
        if (!merged) {
          await this.updateContact(instance, mergeeContact.id, {
            email: mergeeContact.email || null,
            phone_number: mergeeContact.phone_number || null,
            identifier: mergeeContact.identifier || null,
          });
          await this.updateContact(instance, canonicalContact.id, canonicalIdentitySnapshot);
          return (await this.getChatwootContactById(instance, canonicalContact.id)) || canonicalContact;
        }
      }
    }

    const canonicalData = {
      name: canonicalName,
      identifier: canonicalIdentifier,
      ...(canonicalEmail && { email: canonicalEmail }),
      ...(chatwootPhoneNumber && { phone_number: chatwootPhoneNumber }),
    };
    const canonicalUpdated = await this.updateContact(instance, canonicalContact.id, canonicalData);

    if (!canonicalUpdated) {
      this.logger.error(
        `[CW.CONTACT] Unable to apply canonical identity to contact ${canonicalContact.id}: ${JSON.stringify(
          canonicalData,
        )}`,
      );
      if (mergeeContacts.length > 0) {
        await this.updateContact(instance, canonicalContact.id, {
          ...canonicalIdentitySnapshot,
          name: canonicalName,
        });
      }
    }

    await this.addLabelToContact(this.provider.nameInbox, canonicalContact.id);

    return (
      (await this.getChatwootContactById(instance, canonicalContact.id)) || {
        ...canonicalContact,
        ...(canonicalUpdated ? canonicalData : {}),
      }
    );
  }

  public async createConversation(instance: InstanceDto, body: any) {
    if (this.isEditedMessageEnvelope(body)) {
      this.logger.warn(
        `[CW.EDIT] Blocked conversation creation for edited message whatsappId=${body?.key?.id || 'unknown'}`,
      );
      return null;
    }

    const isLid = body.key.addressingMode === 'lid';
    const isGroup = body.key.remoteJid.endsWith('@g.us');
    const phoneNumber = await this.resolveContactPhoneJid(instance, body.key, isGroup);
    const { remoteJid } = body.key;
    const identityJid = isGroup ? remoteJid : phoneNumber;
    const lidJid = isGroup ? null : this.getContactLidJid(body.key);
    const cacheKey = `${instance.instanceName}:createConversation-${identityJid}`;
    const lockKey = `${instance.instanceName}:lock:createConversation-${identityJid}`;
    const reconciliationCacheKey = !isGroup
      ? `${instance.instanceName}:contactIdentityReconciled-${phoneNumber}-${lidJid || ''}`
      : null;
    const maxWaitTime = 5000; // 5 seconds
    const client = await this.clientCw(instance);
    if (!client) return null;

    try {
      this.logger.verbose(`--- Start createConversation ---`);
      this.logger.verbose(`Instance: ${JSON.stringify(instance)}`);

      // If it already exists in the cache, return conversationId
      if (await this.cache.has(cacheKey)) {
        const conversationId = (await this.cache.get(cacheKey)) as number;
        this.logger.verbose(`Found conversation to: ${phoneNumber}, conversation ID: ${conversationId}`);
        let conversationExists: any;
        try {
          conversationExists = await client.conversations.get({
            accountId: this.provider.accountId,
            conversationId: conversationId,
          });
          this.logger.verbose(
            `Conversation exists: ID: ${conversationExists.id} - Name: ${conversationExists.meta.sender.name} - Identifier: ${conversationExists.meta.sender.identifier}`,
          );
          if (isGroup) {
            const storedGroupName = await this.getStoredGroupName(instance.instanceId, remoteJid);
            const desiredGroupName = storedGroupName ? `${storedGroupName} (GROUP)` : null;
            const sender = conversationExists?.meta?.sender;

            if (desiredGroupName && sender?.id && sender?.name !== desiredGroupName) {
              await this.updateContact(instance, sender.id, { name: desiredGroupName });
            }
          }
        } catch (error) {
          this.logger.error(`Error getting conversation: ${error}`);
          conversationExists = false;
        }
        if (!conversationExists) {
          this.logger.verbose('Cached conversation does not exist; continuing without cache');
          await this.cache.delete(cacheKey);
        } else {
          if (reconciliationCacheKey && !(await this.cache.has(reconciliationCacheKey))) {
            const resolvedContactName = await this.resolveContactName(instance, body, phoneNumber);
            const reconciledContact = await this.reconcileChatwootContactIdentity(
              instance,
              phoneNumber,
              lidJid,
              resolvedContactName || body.pushName,
            );
            if (reconciledContact) {
              await this.cache.set(reconciliationCacheKey, true, 5 * 60);
            }
          }
          return conversationId;
        }
      }

      let lockAcquired = await this.cache.setIfNotExists(lockKey, true, 30);

      // If lock already exists, wait until release or timeout
      if (!lockAcquired) {
        this.logger.verbose(`Operação de criação já em andamento para ${remoteJid}, aguardando resultado...`);
        const start = Date.now();
        while (await this.cache.has(lockKey)) {
          if (Date.now() - start > maxWaitTime) {
            this.logger.warn(`Timeout aguardando lock para ${remoteJid}`);
            break;
          }
          await new Promise((res) => setTimeout(res, this.LOCK_POLLING_DELAY_MS));
          if (await this.cache.has(cacheKey)) {
            const conversationId = (await this.cache.get(cacheKey)) as number;
            this.logger.verbose(`Resolves creation of: ${remoteJid}, conversation ID: ${conversationId}`);
            return conversationId;
          }
        }

        lockAcquired = await this.cache.setIfNotExists(lockKey, true, 30);
      }

      if (lockAcquired) {
        this.logger.verbose(`Bloqueio adquirido para: ${lockKey}`);
      } else {
        this.logger.warn(`Lock still active for ${remoteJid}; continuing to avoid dropping the incoming message`);
      }

      try {
        /*
        Double check after lock
        Utilizei uma nova verificação para evitar que outra thread execute entre o terminio do while e o set lock
        */
        if (await this.cache.has(cacheKey)) {
          return (await this.cache.get(cacheKey)) as number;
        }

        const chatId = isGroup ? remoteJid : phoneNumber.split('@')[0].split(':')[0];
        const resolvedContactName = !isGroup ? await this.resolveContactName(instance, body, phoneNumber) : null;
        const chatwootPhoneNumber = !isGroup ? this.getChatwootPhoneNumber(phoneNumber) : null;
        let nameContact =
          resolvedContactName ||
          (!body.key.fromMe && this.isUsableContactName(body.pushName) ? body.pushName.trim() : null) ||
          (chatwootPhoneNumber ? `Contato WhatsApp ${chatwootPhoneNumber}` : chatId);
        const filterInbox = await this.getInbox(instance);
        if (!filterInbox) return null;

        if (isGroup) {
          this.logger.verbose(`Processing group conversation`);
          const storedGroupName = await this.getStoredGroupName(instance.instanceId, chatId);
          nameContact = storedGroupName ? `${storedGroupName} (GROUP)` : `${chatId.split('@')[0]} (GROUP)`;

          try {
            const group = await this.waMonitor.waInstances[instance.instanceName].client.groupMetadata(chatId);
            this.logger.verbose(`Group metadata: JID:${group.JID} - Subject:${group?.subject || group?.Name}`);

            const metadataGroupName = this.normalizeGroupSubject(group?.subject || group?.Name || group?.name);
            if (this.isUsableGroupName(chatId, metadataGroupName)) {
              nameContact = `${metadataGroupName} (GROUP)`;
              await this.persistSyncedGroupNames(instance.instanceId, [{ remoteJid: chatId, name: metadataGroupName }]);
            }
          } catch (error) {
            this.logger.warn(
              `Unable to refresh group metadata for Chatwoot contact ${chatId}: ${error?.toString?.() || error}`,
            );
          }

          const participantJid =
            (isLid && !body.key.fromMe ? body.key.participantAlt : body.key.participant) || body.key.participantAlt;

          if (!participantJid) {
            this.logger.warn(`Group participant JID not found for message ${body.key.id || 'unknown'}`);
          } else {
            const picture_url = await this.waMonitor.waInstances[instance.instanceName].profilePicture(
              participantJid.split('@')[0],
            );
            this.logger.verbose(`Participant profile picture URL: ${JSON.stringify(picture_url)}`);

            const findParticipant = await this.findContact(instance, participantJid.split('@')[0]);

            if (findParticipant) {
              this.logger.verbose(
                `Found participant: ID:${findParticipant.id} - Name: ${findParticipant.name} - identifier: ${findParticipant.identifier}`,
              );
              if (!findParticipant.name || findParticipant.name === chatId) {
                await this.updateContact(instance, findParticipant.id, {
                  name: body.pushName,
                  avatar_url: picture_url.profilePictureUrl || null,
                });
              }
            } else {
              await this.createContact(
                instance,
                participantJid.split('@')[0].split(':')[0],
                filterInbox.id,
                false,
                body.pushName,
                picture_url.profilePictureUrl || null,
                participantJid,
              );
            }
          }
        }

        const picture_url = await this.waMonitor.waInstances[instance.instanceName].profilePicture(chatId);
        this.logger.verbose(`Contact profile picture URL: ${JSON.stringify(picture_url)}`);

        this.logger.verbose(`Searching contact for: ${chatId}`);
        let contact = isGroup
          ? await this.findContact(instance, chatId)
          : await this.reconcileChatwootContactIdentity(instance, phoneNumber, lidJid, nameContact);

        if (contact) {
          this.logger.verbose(`Found contact: ID:${contact.id} - Name:${contact.name}`);
          if (!body.key.fromMe) {
            const waProfilePictureFile =
              picture_url?.profilePictureUrl?.split('#')[0].split('?')[0].split('/').pop() || '';
            const chatwootProfilePictureFile = contact?.thumbnail?.split('#')[0].split('?')[0].split('/').pop() || '';
            const pictureNeedsUpdate = waProfilePictureFile !== chatwootProfilePictureFile;
            const hasUsableGroupName = isGroup && this.isUsableGroupName(chatId, nameContact);
            const nameNeedsUpdate = isGroup
              ? hasUsableGroupName && contact.name !== nameContact
              : this.isUsableContactName(nameContact) && !this.isUsableContactName(contact.name);
            const chatwootPhoneNumber = this.getChatwootPhoneNumber(phoneNumber, contact.identifier);
            const phoneNeedsUpdate = !isGroup && !!chatwootPhoneNumber && contact.phone_number !== chatwootPhoneNumber;
            this.logger.verbose(`Picture needs update: ${pictureNeedsUpdate}`);
            this.logger.verbose(`Name needs update: ${nameNeedsUpdate}`);
            this.logger.verbose(`Phone needs update: ${phoneNeedsUpdate}`);
            if (pictureNeedsUpdate || nameNeedsUpdate || phoneNeedsUpdate) {
              await this.updateContact(instance, contact.id, {
                ...(nameNeedsUpdate && { name: nameContact }),
                ...(phoneNeedsUpdate && { phone_number: chatwootPhoneNumber }),
                ...(waProfilePictureFile === '' && { avatar: null }),
                ...(pictureNeedsUpdate && { avatar_url: picture_url?.profilePictureUrl }),
              });
            }
          }
        } else {
          contact = await this.createContact(
            instance,
            chatId,
            filterInbox.id,
            isGroup,
            nameContact,
            picture_url.profilePictureUrl || null,
            lidJid || phoneNumber,
          );
        }

        if (!contact) {
          this.logger.warn(`Contact not created or found`);
          return null;
        }

        const contactId = contact?.payload?.id || contact?.payload?.contact?.id || contact?.id;
        this.logger.verbose(`Contact ID: ${contactId}`);
        if (!contactId) {
          this.logger.warn('Contact ID not found');
          return null;
        }
        if (reconciliationCacheKey) {
          await this.cache.set(reconciliationCacheKey, true, 5 * 60);
        }

        const contactCacheKey = `${instance.instanceName}:createConversation-${filterInbox.id}-${contactId}`;

        if (await this.cache.has(contactCacheKey)) {
          const conversationId = (await this.cache.get(contactCacheKey)) as number;
          try {
            await client.conversations.get({
              accountId: this.provider.accountId,
              conversationId,
            });
            this.logger.verbose(`Found cached conversation for contact ${contactId}: ${conversationId}`);
            await this.cache.set(cacheKey, conversationId, 1800);
            return conversationId;
          } catch {
            this.logger.warn(`Cached conversation ${conversationId} for contact ${contactId} is not available`);
            await this.cache.delete(contactCacheKey);
          }
        }

        const contactConversations = (await client.contacts.listConversations({
          accountId: this.provider.accountId,
          id: contactId,
        })) as any;

        if (!contactConversations || !contactConversations.payload) {
          this.logger.error(`No conversations found or payload is undefined`);
          return null;
        }

        const inboxConversation = this.findReusableInboxConversation(
          contactConversations.payload,
          filterInbox.id,
          this.provider.reopenConversation || body.key.fromMe,
        );
        if (inboxConversation) {
          if (this.provider.reopenConversation) {
            this.logger.verbose(
              `Found conversation in reopenConversation mode: ID: ${inboxConversation.id} - Name: ${inboxConversation.meta.sender.name} - Identifier: ${inboxConversation.meta.sender.identifier}`,
            );
            if (inboxConversation && this.provider.conversationPending && inboxConversation.status !== 'open') {
              await client.conversations.toggleStatus({
                accountId: this.provider.accountId,
                conversationId: inboxConversation.id,
                data: {
                  status: 'pending',
                },
              });
            }
          } else {
            this.logger.verbose(`Found conversation: ${JSON.stringify(inboxConversation)}`);
          }

          if (inboxConversation) {
            this.logger.verbose(`Returning existing conversation ID: ${inboxConversation.id}`);
            await this.cache.set(cacheKey, inboxConversation.id, 1800);
            await this.cache.set(contactCacheKey, inboxConversation.id, 1800);
            return inboxConversation.id;
          }
        }

        if (this.provider.reopenConversation) {
          const storedConversation = await this.findLatestConversationInChatwootDb(contactId, filterInbox.id);

          if (storedConversation?.id) {
            this.logger.verbose(
              `Found conversation in Chatwoot database: ID: ${storedConversation.id} - Contact ID: ${contactId}`,
            );

            if (this.provider.conversationPending && storedConversation.status !== 0) {
              await client.conversations.toggleStatus({
                accountId: this.provider.accountId,
                conversationId: storedConversation.id,
                data: {
                  status: 'pending',
                },
              });
            }

            await this.cache.set(cacheKey, storedConversation.id, 1800);
            await this.cache.set(contactCacheKey, storedConversation.id, 1800);
            return storedConversation.id;
          }
        }

        const data = {
          contact_id: contactId.toString(),
          inbox_id: filterInbox.id.toString(),
        };

        if (this.provider.conversationPending) {
          data['status'] = 'pending';
        }

        const conversation = await client.conversations.create({
          accountId: this.provider.accountId,
          data,
        });

        if (!conversation) {
          this.logger.warn(`Conversation not created or found`);
          return null;
        }

        this.logger.verbose(`New conversation created of ${remoteJid} with ID: ${conversation.id}`);
        await this.cache.set(cacheKey, conversation.id, 1800);
        await this.cache.set(contactCacheKey, conversation.id, 1800);
        return conversation.id;
      } finally {
        if (lockAcquired) {
          await this.cache.delete(lockKey);
          this.logger.verbose(`Block released for: ${lockKey}`);
        }
      }
    } catch (error) {
      const errorDetails = error instanceof Error ? error.stack || error.message : JSON.stringify(error);
      this.logger.error(`Error in createConversation: ${errorDetails}`);
      return null;
    }
  }

  private async findLatestConversationInChatwootDb(contactId: number, inboxId: number) {
    try {
      const result = await this.pgClient.query(
        `SELECT id, status
         FROM conversations
         WHERE account_id = $1
           AND inbox_id = $2
           AND contact_id = $3
         ORDER BY last_activity_at DESC NULLS LAST, updated_at DESC, id DESC
         LIMIT 1`,
        [this.provider.accountId, inboxId, contactId],
      );

      return result?.rows?.[0] || null;
    } catch (error) {
      this.logger.warn(`Unable to find latest Chatwoot conversation in database: ${error?.toString?.() || error}`);
      return null;
    }
  }

  public async getInbox(instance: InstanceDto): Promise<inbox | null> {
    const cacheKey = `${instance.instanceName}:getInbox`;
    if (await this.cache.has(cacheKey)) {
      return (await this.cache.get(cacheKey)) as inbox;
    }

    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const inbox = (await client.inboxes.list({
      accountId: this.provider.accountId,
    })) as any;

    if (!inbox) {
      this.logger.warn('inbox not found');
      return null;
    }

    const findByName = inbox.payload.find((inbox) => inbox.name === this.getClientCwConfig().nameInbox);

    if (!findByName) {
      this.logger.warn('inbox not found');
      return null;
    }

    await this.cache.set(cacheKey, findByName);
    return findByName;
  }

  public async createMessage(
    instance: InstanceDto,
    conversationId: number,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    privateMessage?: boolean,
    attachments?: {
      content: unknown;
      encoding: string;
      filename: string;
    }[],
    messageBody?: any,
    sourceId?: string,
    quotedMsg?: MessageModel,
  ) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const replyToIds = await this.getReplyToIds(messageBody, instance);
    const sourceReplyId = quotedMsg?.chatwootMessageId || null;
    const messageContent = this.addMissingReplyFallback(
      content,
      messageBody,
      !!sourceReplyId || !!replyToIds.in_reply_to,
    );

    const message = await client.messages.create({
      accountId: this.provider.accountId,
      conversationId: conversationId,
      data: {
        content: messageContent,
        message_type: messageType,
        attachments: attachments,
        private: privateMessage || false,
        source_id: sourceId,
        content_attributes: {
          ...replyToIds,
        },
        source_reply_id: sourceReplyId ? sourceReplyId.toString() : null,
      },
    });

    if (!message) {
      this.logger.warn('message not found');
      return null;
    }

    if (sourceId) {
      await this.cacheChatwootMessageReference(instance, sourceId.replace(/^WAID:/, ''), {
        chatwootMessageId: Number((message as any).id),
        chatwootConversationId: Number((message as any).conversation_id || conversationId),
        chatwootInboxId: Number((message as any).inbox_id) || undefined,
      });
    }

    return message;
  }

  public async getOpenConversationByContact(
    instance: InstanceDto,
    inbox: inbox,
    contact: generic_id & contact,
  ): Promise<conversation> {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const conversations = (await client.contacts.listConversations({
      accountId: this.provider.accountId,
      id: contact.id,
    })) as any;

    return (
      conversations.payload.find(
        (conversation) => conversation.inbox_id === inbox.id && conversation.status === 'open',
      ) || undefined
    );
  }

  public async createBotMessage(
    instance: InstanceDto,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    attachments?: {
      content: unknown;
      encoding: string;
      filename: string;
    }[],
  ) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const contact = await this.findContact(instance, '123456');

    if (!contact) {
      this.logger.warn('contact not found');
      return null;
    }

    const filterInbox = await this.getInbox(instance);

    if (!filterInbox) {
      this.logger.warn('inbox not found');
      return null;
    }

    const conversation = await this.getOpenConversationByContact(instance, filterInbox, contact);

    if (!conversation) {
      this.logger.warn('conversation not found');
      return;
    }

    const message = await client.messages.create({
      accountId: this.provider.accountId,
      conversationId: conversation.id,
      data: {
        content: content,
        message_type: messageType,
        attachments: attachments,
      },
    });

    if (!message) {
      this.logger.warn('message not found');
      return null;
    }

    return message;
  }

  private async sendData(
    conversationId: number,
    fileStream: Readable,
    fileName: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    content?: string,
    instance?: InstanceDto,
    messageBody?: any,
    sourceId?: string,
    quotedMsg?: MessageModel,
  ) {
    if (sourceId && this.isImportHistoryAvailable()) {
      const messageAlreadySaved = await chatwootImport.getExistingSourceIds([sourceId], { conversationId });
      if (messageAlreadySaved) {
        if (messageAlreadySaved.size > 0) {
          this.logger.warn('Message already saved on chatwoot');
          return null;
        }
      }
    }
    const data = new FormData();

    data.append('message_type', messageType);

    data.append('attachments[]', fileStream, { filename: fileName });

    const sourceReplyId = quotedMsg?.chatwootMessageId || null;
    let replyToIds: { in_reply_to: string; in_reply_to_external_id: string } = {
      in_reply_to: null,
      in_reply_to_external_id: null,
    };

    if (messageBody && instance) {
      replyToIds = await this.getReplyToIds(messageBody, instance);

      if (replyToIds.in_reply_to || replyToIds.in_reply_to_external_id) {
        const contentAttributes = JSON.stringify({
          ...replyToIds,
        });
        data.append('content_attributes', contentAttributes);
      }
    }

    const messageContent = this.addMissingReplyFallback(
      content || '',
      messageBody,
      !!sourceReplyId || !!replyToIds.in_reply_to,
    );
    if (messageContent) {
      data.append('content', messageContent);
    }

    if (sourceReplyId) {
      data.append('source_reply_id', sourceReplyId.toString());
    }

    if (sourceId) {
      data.append('source_id', sourceId);
    }

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: `${this.provider.url}/api/v1/accounts/${this.provider.accountId}/conversations/${conversationId}/messages`,
      headers: {
        api_access_token: this.provider.token,
        ...data.getHeaders(),
      },
      data: data,
    };

    try {
      const { data } = await axios.request(config);

      if (instance && sourceId) {
        await this.cacheChatwootMessageReference(instance, sourceId.replace(/^WAID:/, ''), {
          chatwootMessageId: Number(data?.id),
          chatwootConversationId: Number(data?.conversation_id || conversationId),
          chatwootInboxId: Number(data?.inbox_id) || undefined,
        });
      }

      return data;
    } catch (error) {
      this.logger.error(`Error creating Chatwoot attachment message: ${this.formatError(error)}`);
    }
  }

  public async createBotQr(
    instance: InstanceDto,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    fileStream?: Readable,
    fileName?: string,
  ) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    if (!this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT) {
      this.logger.log('Chatwoot bot contact is disabled');

      return true;
    }

    const contact = await this.findContact(instance, '123456');

    if (!contact) {
      this.logger.warn('contact not found');
      return null;
    }

    const filterInbox = await this.getInbox(instance);

    if (!filterInbox) {
      this.logger.warn('inbox not found');
      return null;
    }

    const conversation = await this.getOpenConversationByContact(instance, filterInbox, contact);

    if (!conversation) {
      this.logger.warn('conversation not found');
      return;
    }

    const data = new FormData();

    if (content) {
      data.append('content', content);
    }

    data.append('message_type', messageType);

    if (fileStream && fileName) {
      data.append('attachments[]', fileStream, { filename: fileName });
    }

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: `${this.provider.url}/api/v1/accounts/${this.provider.accountId}/conversations/${conversation.id}/messages`,
      headers: {
        api_access_token: this.provider.token,
        ...data.getHeaders(),
      },
      data: data,
    };

    try {
      const { data } = await axios.request(config);

      return data;
    } catch (error) {
      this.logger.error(`Error creating Chatwoot bot QR message: ${this.formatError(error)}`);
    }
  }

  public async sendAttachment(waInstance: any, number: string, media: any, caption?: string, options?: Options) {
    try {
      if (!waInstance) {
        throw new Error('WhatsApp instance not found');
      }

      if (!media || typeof media !== 'string') {
        throw new Error('Attachment URL is invalid');
      }

      const parsedMedia = path.parse(decodeURIComponent(media));
      let mimeType = mimeTypes.lookup(parsedMedia?.ext) || '';
      let fileName = parsedMedia?.name + parsedMedia?.ext;

      if (!mimeType) {
        const parts = media.split('/');
        fileName = decodeURIComponent(parts[parts.length - 1]);

        const response = await axios.get(media, {
          responseType: 'arraybuffer',
          timeout: 30_000,
        });
        mimeType = response.headers['content-type'];
      }

      let type = 'document';

      switch (mimeType.split('/')[0]) {
        case 'image':
          type = 'image';
          break;
        case 'video':
          type = 'video';
          break;
        case 'audio':
          type = 'audio';
          break;
        default:
          type = 'document';
          break;
      }

      if (type === 'audio') {
        const data: SendAudioDto = {
          number: number,
          audio: media,
          delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
          quoted: options?.quoted,
        };

        sendTelemetry('/message/sendWhatsAppAudio');

        const messageSent = await waInstance?.audioWhatsapp(data, null, true);

        return messageSent;
      }

      const documentExtensions = ['.gif', '.svg', '.tiff', '.tif', '.dxf', '.dwg'];
      if (type === 'image' && parsedMedia && documentExtensions.includes(parsedMedia?.ext)) {
        type = 'document';
      }

      const data: SendMediaDto = {
        number: number,
        mediatype: type as any,
        fileName: fileName,
        media: media,
        delay: 1200,
        quoted: options?.quoted,
      };

      sendTelemetry('/message/sendMedia');

      if (caption) {
        data.caption = caption;
      }

      const messageSent = await waInstance?.mediaMessage(data, null, true);

      return messageSent;
    } catch (error) {
      this.logger.error(`Error sending Chatwoot attachment to WhatsApp: ${this.formatError(error)}`);
      throw error; // Re-throw para que o erro seja tratado pelo caller
    }
  }

  private getChatwootSenderDestination(sender?: any): string | null {
    if (!sender) {
      return null;
    }

    const identifier = sender.identifier?.toString();
    const phoneNumber = sender.phone_number?.toString().replace(/\D/g, '');

    if (identifier?.endsWith('@g.us')) {
      return identifier;
    }

    if (phoneNumber) {
      return phoneNumber;
    }

    return identifier || null;
  }

  public async onSendMessageError(instance: InstanceDto, conversation: number, error?: any) {
    this.logger.verbose(`onSendMessageError ${this.formatError(error)}`);

    const client = await this.clientCw(instance);

    if (!client) {
      return;
    }

    if (error && error?.status === 400 && error?.message[0]?.exists === false) {
      await client.messages.create({
        accountId: this.provider.accountId,
        conversationId: conversation,
        data: {
          content: `${i18next.t('cw.message.numbernotinwhatsapp')}`,
          message_type: 'outgoing',
          private: true,
        },
      });

      return;
    }

    await client.messages.create({
      accountId: this.provider.accountId,
      conversationId: conversation,
      data: {
        content: i18next.t('cw.message.notsent', {
          error: error ? `_${error.toString()}_` : '',
        }),
        message_type: 'outgoing',
        private: true,
      },
    });
  }

  public async receiveWebhook(instance: InstanceDto, body: any) {
    try {
      await new Promise((resolve) => setTimeout(resolve, 500));

      const client = await this.clientCw(instance);

      if (!client) {
        this.logger.warn('client not found');
        return null;
      }

      if (
        this.provider.reopenConversation === false &&
        body.event === 'conversation_status_changed' &&
        body.status === 'resolved' &&
        body.meta?.sender?.identifier
      ) {
        const keyToDelete = `${instance.instanceName}:createConversation-${body.meta.sender.identifier}`;
        await this.cache.delete(keyToDelete);
      }

      if (
        !body?.conversation ||
        body.private ||
        (body.event === 'message_updated' && !body.content_attributes?.deleted)
      ) {
        return { message: 'bot' };
      }

      const chatId = this.getChatwootSenderDestination(body.conversation?.meta?.sender);
      if (!chatId) {
        this.logger.warn('Chatwoot sender destination not found');
        return { message: 'bot' };
      }

      // Chatwoot to Whatsapp
      const messageReceived =
        typeof body.content === 'string'
          ? body.content
              .replaceAll(/(?<!\*)\*((?!\s)([^\n*]+?)(?<!\s))\*(?!\*)/g, '_$1_') // Substitui * por _
              .replaceAll(/\*{2}((?!\s)([^\n*]+?)(?<!\s))\*{2}/g, '*$1*') // Substitui ** por *
              .replaceAll(/~{2}((?!\s)([^\n*]+?)(?<!\s))~{2}/g, '~$1~') // Substitui ~~ por ~
              .replaceAll(/(?<!`)`((?!\s)([^`*]+?)(?<!\s))`(?!`)/g, '```$1```') // Substitui ` por ```
          : '';

      const senderName = body?.sender?.available_name || body?.sender?.name;
      const waInstance = this.waMonitor.waInstances[instance.instanceName];
      if (!waInstance) {
        this.logger.warn(`WhatsApp instance ${instance.instanceName} not found while processing Chatwoot webhook`);
        if (body.message_type === 'outgoing' && body.conversation?.id) {
          await this.onSendMessageError(instance, body.conversation.id, 'Instance not found');
        }
        return { message: 'bot' };
      }
      instance.instanceId = waInstance.instanceId;

      if (body.event === 'message_updated' && body.content_attributes?.deleted) {
        const messages = await this.prismaRepository.message.findMany({
          where: {
            chatwootMessageId: body.id,
            instanceId: instance.instanceId,
          },
        });

        for (const message of messages) {
          const key = message.key as WAMessageKey;
          if (!key?.remoteJid) {
            this.logger.warn(`Cannot delete WhatsApp message ${message.id}: remoteJid not found`);
            continue;
          }

          try {
            await waInstance.client.sendMessage(key.remoteJid, { delete: key });
            await this.prismaRepository.message.deleteMany({ where: { id: message.id } });
          } catch (error) {
            this.logger.error(
              `Error deleting WhatsApp message ${key.id || message.id} from Chatwoot: ${this.formatError(error)}`,
            );
          }
        }
        return { message: 'bot' };
      }

      const cwBotContact = this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT;

      if (chatId === '123456' && body.message_type === 'outgoing') {
        const command = (messageReceived ?? '')
          .replace(/^\s*\//, '')
          .trim()
          .toLowerCase();

        if (cwBotContact && (command.includes('init') || command.includes('iniciar'))) {
          const state = waInstance?.connectionStatus?.state;

          if (state !== 'open') {
            const number = command.split(':')[1];
            await waInstance.connectToWhatsapp(number);
          } else {
            await this.createBotMessage(
              instance,
              i18next.t('cw.inbox.alreadyConnected', {
                inboxName: body.inbox.name,
              }),
              'incoming',
            );
          }
        }

        if (command === 'clearcache') {
          waInstance.clearCacheChatwoot();
          await this.createBotMessage(
            instance,
            i18next.t('cw.inbox.clearCache', {
              inboxName: body.inbox.name,
            }),
            'incoming',
          );
        }

        if (command === 'status') {
          const state = waInstance?.connectionStatus?.state;

          if (!state) {
            await this.createBotMessage(
              instance,
              i18next.t('cw.inbox.notFound', {
                inboxName: body.inbox.name,
              }),
              'incoming',
            );
          }

          if (state) {
            await this.createBotMessage(
              instance,
              i18next.t('cw.inbox.status', {
                inboxName: body.inbox.name,
                state: state,
              }),
              'incoming',
            );
          }
        }

        if (cwBotContact && (command === 'disconnect' || command === 'desconectar')) {
          const msgLogout = i18next.t('cw.inbox.disconnect', {
            inboxName: body.inbox.name,
          });

          await this.createBotMessage(instance, msgLogout, 'incoming');

          await waInstance?.logoutInstance();
        }
      }

      if (
        body.event === 'message_created' &&
        body.message_type === 'outgoing' &&
        body?.conversation &&
        chatId !== '123456'
      ) {
        if (body?.source_id?.substring(0, 5) === 'WAID:') {
          return { message: 'bot' };
        }

        let formatText: string;
        if (senderName === null || senderName === undefined) {
          formatText = messageReceived;
        } else {
          const formattedDelimiter = this.provider.signDelimiter
            ? this.provider.signDelimiter.replaceAll('\\n', '\n')
            : '\n';
          const textToConcat = this.provider.signMsg ? [`*${senderName}:*`] : [];
          textToConcat.push(messageReceived);

          formatText = textToConcat.join(formattedDelimiter);
        }

        const currentConversationMessage = body.conversation.messages?.find((message) => message.id === body.id);
        const attachments = Array.isArray(body.attachments)
          ? body.attachments
          : currentConversationMessage?.attachments || [];
        const quotedMessage = await this.getQuotedMessage(body, instance);
        const deliveryMessageId = body.id || body.source_id || `${body.conversation.id}:${body.created_at}`;

        if (attachments.length > 0) {
          this.logger.info(
            `[CW.ATTACHMENT] Sending ${attachments.length} attachment(s) from Chatwoot message ${body.id}`,
          );

          for (const [index, attachment] of attachments.entries()) {
            const mediaUrl = attachment.data_url || attachment.file_url;
            const caption = index === 0 && messageReceived ? formatText : undefined;
            const deliveryKey = `${instance.instanceName}:chatwoot-outgoing:${deliveryMessageId}:attachment:${index}`;

            if (!(await this.beginOutgoingDelivery(deliveryKey))) {
              this.logger.warn(
                `[CW.ATTACHMENT] Skipping duplicate attachment ${index + 1}/${attachments.length} from message ${body.id}`,
              );
              continue;
            }

            if (!mediaUrl) {
              this.logger.error(
                `[CW.ATTACHMENT] Attachment ${index + 1}/${attachments.length} has no media URL (message ${body.id})`,
              );
              await this.finishOutgoingDelivery(deliveryKey, false);
              continue;
            }

            let attachmentSent = false;
            try {
              const messageSent = await this.sendAttachment(waInstance, chatId, mediaUrl, caption, {
                quoted: quotedMessage,
              });

              if (!messageSent) {
                throw new Error('Attachment not sent');
              }

              await this.updateChatwootMessageId(
                {
                  ...messageSent,
                },
                {
                  messageId: body.id,
                  inboxId: body.inbox?.id,
                  conversationId: body.conversation?.id,
                  contactInboxSourceId: body.conversation?.contact_inbox?.source_id,
                },
                instance,
              );

              this.logger.info(
                `[CW.ATTACHMENT] Sent attachment ${index + 1}/${attachments.length} from Chatwoot message ${body.id}`,
              );
              attachmentSent = true;
            } catch (error) {
              const errorDetails = error instanceof Error ? error.stack || error.message : JSON.stringify(error);
              this.logger.error(
                `[CW.ATTACHMENT] Failed attachment ${index + 1}/${attachments.length} from Chatwoot message ${
                  body.id
                }: ${errorDetails}`,
              );
              if (body.conversation?.id) {
                await this.onSendMessageError(instance, body.conversation.id, error);
              }
            } finally {
              await this.finishOutgoingDelivery(deliveryKey, attachmentSent);
            }
          }
        } else {
          const deliveryKey = `${instance.instanceName}:chatwoot-outgoing:${deliveryMessageId}:text`;
          if (!(await this.beginOutgoingDelivery(deliveryKey))) {
            this.logger.warn(`[CW.OUTGOING] Skipping duplicate text message ${body.id}`);
            return { message: 'bot' };
          }

          const data: SendTextDto = {
            number: chatId,
            text: formatText,
            delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
            quoted: quotedMessage,
          };

          sendTelemetry('/message/sendText');

          let messageSent: any;
          let textSent = false;
          try {
            messageSent = await waInstance?.textMessage(data, true);
            if (!messageSent) {
              throw new Error('Message not sent');
            }

            if (Long.isLong(messageSent?.messageTimestamp)) {
              messageSent.messageTimestamp = messageSent.messageTimestamp?.toNumber();
            }

            await this.updateChatwootMessageId(
              {
                ...messageSent,
              },
              {
                messageId: body.id,
                inboxId: body.inbox?.id,
                conversationId: body.conversation?.id,
                contactInboxSourceId: body.conversation?.contact_inbox?.source_id,
              },
              instance,
            );
            textSent = true;
          } catch (error) {
            if (!messageSent && body.conversation?.id) {
              await this.onSendMessageError(instance, body.conversation?.id, error);
            }
            throw error;
          } finally {
            await this.finishOutgoingDelivery(deliveryKey, textSent);
          }
        }

        const chatwootRead = this.configService.get<Chatwoot>('CHATWOOT').MESSAGE_READ;
        if (chatwootRead) {
          const lastMessage = await this.prismaRepository.message.findFirst({
            where: {
              key: {
                path: ['fromMe'],
                equals: false,
              },
              instanceId: instance.instanceId,
              chatwootConversationId: body.conversation.id,
            },
            orderBy: { messageTimestamp: 'desc' },
          });
          if (lastMessage && !lastMessage.chatwootIsRead) {
            const key = lastMessage.key as WAMessageKey;

            await waInstance?.markMessageAsRead({
              readMessages: [
                {
                  id: key.id,
                  fromMe: key.fromMe,
                  remoteJid: key.remoteJid,
                },
              ],
            });
            const updateMessage = {
              chatwootMessageId: lastMessage.chatwootMessageId,
              chatwootConversationId: lastMessage.chatwootConversationId,
              chatwootInboxId: lastMessage.chatwootInboxId,
              chatwootContactInboxSourceId: lastMessage.chatwootContactInboxSourceId,
              chatwootIsRead: true,
            };

            await this.prismaRepository.message.updateMany({
              where: {
                instanceId: instance.instanceId,
                key: {
                  path: ['id'],
                  equals: key.id,
                },
              },
              data: updateMessage,
            });
          }
        }
      }

      if (body.message_type === 'template' && body.event === 'message_created') {
        if (typeof body.content !== 'string' || !body.content.trim()) {
          this.logger.warn(`Chatwoot template message ${body.id || 'unknown'} has no text content`);
          return { message: 'bot' };
        }

        const data: SendTextDto = {
          number: chatId,
          text: body.content.replace(/\\\r\n|\\\n|\n/g, '\n'),
          delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
        };

        sendTelemetry('/message/sendText');

        await waInstance?.textMessage(data);
      }

      return { message: 'bot' };
    } catch (error) {
      this.logger.error(
        `Error processing Chatwoot webhook instance=${instance.instanceName}: ${this.formatError(error)}`,
      );

      return { message: 'bot' };
    }
  }

  private async updateChatwootMessageId(
    message: MessageModel,
    chatwootMessageIds: ChatwootMessage,
    instance: InstanceDto,
  ) {
    const key = message.key as WAMessageKey;

    if (!chatwootMessageIds.messageId || !key?.id) {
      return;
    }

    // Use raw SQL to avoid JSON path issues
    const result = await this.prismaRepository.$executeRaw`
      UPDATE "Message" 
      SET 
        "chatwootMessageId" = ${chatwootMessageIds.messageId},
        "chatwootConversationId" = ${chatwootMessageIds.conversationId},
        "chatwootInboxId" = ${chatwootMessageIds.inboxId},
        "chatwootContactInboxSourceId" = ${chatwootMessageIds.contactInboxSourceId},
        "chatwootIsRead" = ${chatwootMessageIds.isRead || false}
      WHERE "instanceId" = ${instance.instanceId} 
      AND "key"->>'id' = ${key.id}
    `;

    this.logger.verbose(`Update result: ${result} rows affected`);

    if (this.isImportHistoryAvailable()) {
      try {
        await chatwootImport.updateMessageSourceID(chatwootMessageIds.messageId, key.id);
      } catch (error) {
        this.logger.error(`Error updating Chatwoot message source ID: ${error}`);
      }
    }

    await this.cacheChatwootMessageReference(instance, key.id, {
      chatwootMessageId: chatwootMessageIds.messageId,
      chatwootConversationId: chatwootMessageIds.conversationId,
      chatwootInboxId: chatwootMessageIds.inboxId,
      chatwootContactInboxSourceId: chatwootMessageIds.contactInboxSourceId,
    });
  }

  private async getMessageByKeyId(instance: InstanceDto, keyId: string): Promise<MessageModel> {
    // Use raw SQL query to avoid JSON path issues with Prisma
    const messages = await this.prismaRepository.$queryRaw`
      SELECT * FROM "Message" 
      WHERE "instanceId" = ${instance.instanceId} 
      AND "key"->>'id' = ${keyId}
      LIMIT 1
    `;

    return (messages as MessageModel[])[0] || null;
  }

  private getChatwootMessageReferenceCacheKey(instance: InstanceDto, keyId: string): string {
    return `${instance.instanceName}:chatwootMessageReference-${keyId}`;
  }

  private async cacheChatwootMessageReference(
    instance: InstanceDto,
    keyId: string,
    reference: Partial<ChatwootMessageReference>,
  ): Promise<void> {
    const chatwootMessageId = Number(reference.chatwootMessageId);
    const chatwootConversationId = Number(reference.chatwootConversationId);

    if (!keyId || !Number.isFinite(chatwootMessageId) || !Number.isFinite(chatwootConversationId)) {
      return;
    }

    await this.cache.set(
      this.getChatwootMessageReferenceCacheKey(instance, keyId),
      {
        chatwootMessageId,
        chatwootConversationId,
        chatwootInboxId: Number(reference.chatwootInboxId) || undefined,
        chatwootContactInboxSourceId: reference.chatwootContactInboxSourceId,
      } satisfies ChatwootMessageReference,
      24 * 60 * 60,
    );
  }

  private async findChatwootMessageReferenceBySourceId(
    instance: InstanceDto,
    keyId: string,
  ): Promise<ChatwootMessageReference | null> {
    try {
      const inbox = await this.getInbox(instance);
      if (!inbox?.id) {
        return null;
      }

      const result = await this.pgClient.query(
        `SELECT id, conversation_id, inbox_id
         FROM messages
         WHERE account_id = $1
           AND inbox_id = $2
           AND source_id::text = ANY($3::text[])
         ORDER BY created_at DESC, id DESC
         LIMIT 1`,
        [this.provider.accountId, inbox.id, [`WAID:${keyId}`, keyId]],
      );
      const row = result?.rows?.[0];

      if (!row?.id || !row?.conversation_id) {
        return null;
      }

      return {
        chatwootMessageId: Number(row.id),
        chatwootConversationId: Number(row.conversation_id),
        chatwootInboxId: Number(row.inbox_id) || undefined,
      };
    } catch (error) {
      this.logger.warn(
        `[CW.EDIT] Unable to find Chatwoot message by source_id WAID:${keyId}: ${error?.toString?.() || error}`,
      );
      return null;
    }
  }

  private async findExistingChatwootMessageReference(
    instance: InstanceDto,
    keyId: string,
  ): Promise<ChatwootMessageReference | null> {
    if (!keyId) {
      return null;
    }

    const localMessage = await this.getMessageByKeyId(instance, keyId);
    if (localMessage?.chatwootMessageId && localMessage?.chatwootConversationId) {
      return {
        chatwootMessageId: localMessage.chatwootMessageId,
        chatwootConversationId: localMessage.chatwootConversationId,
        chatwootInboxId: localMessage.chatwootInboxId || undefined,
        chatwootContactInboxSourceId: localMessage.chatwootContactInboxSourceId || undefined,
      };
    }

    const cachedReference = (await this.cache.get(
      this.getChatwootMessageReferenceCacheKey(instance, keyId),
    )) as ChatwootMessageReference | null;
    if (cachedReference?.chatwootMessageId && cachedReference?.chatwootConversationId) {
      return cachedReference;
    }

    if (!this.isImportHistoryAvailable()) {
      return null;
    }

    return await this.findChatwootMessageReferenceBySourceId(instance, keyId);
  }

  private async resolveChatwootMessageReference(
    instance: InstanceDto,
    keyId: string,
    fallbackKey?: WAMessageKey,
  ): Promise<(Partial<MessageModel> & ChatwootMessageReference) | null> {
    let localMessage = await this.getMessageByKeyId(instance, keyId);

    for (
      let attempt = 1;
      attempt <= 5 && (!localMessage || !localMessage.chatwootMessageId || !localMessage.chatwootConversationId);
      attempt++
    ) {
      await new Promise((resolve) => setTimeout(resolve, 400));
      localMessage = await this.getMessageByKeyId(instance, keyId);
    }

    if (localMessage?.chatwootMessageId && localMessage?.chatwootConversationId) {
      const reference = {
        chatwootMessageId: localMessage.chatwootMessageId,
        chatwootConversationId: localMessage.chatwootConversationId,
        chatwootInboxId: localMessage.chatwootInboxId || undefined,
        chatwootContactInboxSourceId: localMessage.chatwootContactInboxSourceId || undefined,
      };
      await this.cacheChatwootMessageReference(instance, keyId, reference);
      return { ...localMessage, ...reference };
    }

    const cachedReference = (await this.cache.get(
      this.getChatwootMessageReferenceCacheKey(instance, keyId),
    )) as ChatwootMessageReference | null;
    const reference = cachedReference || (await this.findChatwootMessageReferenceBySourceId(instance, keyId));

    if (!reference?.chatwootMessageId || !reference?.chatwootConversationId) {
      return null;
    }

    await this.cacheChatwootMessageReference(instance, keyId, reference);

    if (localMessage?.id) {
      await this.prismaRepository.message.update({
        where: { id: localMessage.id },
        data: {
          chatwootMessageId: reference.chatwootMessageId,
          chatwootConversationId: reference.chatwootConversationId,
          chatwootInboxId: reference.chatwootInboxId,
          chatwootContactInboxSourceId: reference.chatwootContactInboxSourceId,
        },
      });
    }

    this.logger.info(
      `[CW.EDIT] Recovered Chatwoot association by WAID whatsappId=${keyId} chatwootMessageId=${reference.chatwootMessageId}`,
    );

    return {
      ...(localMessage || {}),
      key: (localMessage?.key || fallbackKey) as any,
      ...reference,
    };
  }

  public async ensureChatwootMessageReference(
    instance: InstanceDto,
    keyId: string,
    fallbackKey?: WAMessageKey,
  ): Promise<boolean> {
    if (!keyId || !(await this.clientCw(instance))) {
      return false;
    }

    return !!(await this.resolveChatwootMessageReference(instance, keyId, fallbackKey));
  }

  private getChatwootMessageStatusName(status: number): 'sent' | 'delivered' | 'read' | 'failed' | null {
    return (
      (
        {
          0: 'sent',
          1: 'delivered',
          2: 'read',
          3: 'failed',
        } as const
      )[status] ?? null
    );
  }

  private async updateChatwootMessageContent(
    client: ChatwootClient,
    reference: ChatwootMessageReference,
    content: string,
  ): Promise<{ method: 'api' | 'database'; realtimeNotified: boolean; response: any }> {
    let apiResponse: any;

    try {
      apiResponse = await client.messages.update({
        accountId: this.provider.accountId,
        conversationId: reference.chatwootConversationId,
        messageId: reference.chatwootMessageId,
        data: { content },
      });

      const apiMessage = apiResponse?.payload || apiResponse?.data || apiResponse;
      if (apiMessage?.content === content) {
        return { method: 'api', realtimeNotified: true, response: apiResponse };
      }

      this.logger.warn(
        `[CW.EDIT] Chatwoot API ignored message content chatwootMessageId=${reference.chatwootMessageId}; using verified database fallback`,
      );
    } catch (error) {
      this.logger.warn(
        `[CW.EDIT] Chatwoot API content update unavailable chatwootMessageId=${
          reference.chatwootMessageId
        }; using database fallback: ${this.formatError(error)}`,
      );
    }

    if (!this.isImportHistoryAvailable()) {
      throw new Error(
        'Chatwoot did not persist the edited content and CHATWOOT_IMPORT_DATABASE_CONNECTION_URI is not configured',
      );
    }

    const updateResult = await this.pgClient.query(
      `WITH target AS (
         SELECT id, status AS original_status, content_attributes->'external_error' AS external_error
         FROM messages
         WHERE id = $2
           AND conversation_id = $3
           AND account_id = $4
         FOR UPDATE
       )
       UPDATE messages AS message
       SET content = $1,
           processed_message_content = $1,
           updated_at = NOW(),
           status = CASE WHEN target.original_status = 0 THEN 1 ELSE 0 END
       FROM target
       WHERE message.id = target.id
       RETURNING
         message.id,
         message.content,
         message.processed_message_content,
         message.status,
         target.original_status,
         target.external_error`,
      [content, reference.chatwootMessageId, reference.chatwootConversationId, this.provider.accountId],
    );
    const updatedRow = updateResult?.rows?.[0];

    if (
      !updatedRow ||
      updatedRow.content !== content ||
      updatedRow.processed_message_content !== content ||
      Number(updatedRow.id) !== Number(reference.chatwootMessageId)
    ) {
      throw new Error(`Chatwoot database did not persist edited message ${reference.chatwootMessageId}`);
    }

    const originalStatus = Number(updatedRow.original_status);
    const originalStatusName = this.getChatwootMessageStatusName(originalStatus);
    let realtimeNotified = false;
    let realtimeResponse: any = apiResponse;

    try {
      if (!originalStatusName) {
        throw new Error(`Unknown original Chatwoot message status ${updatedRow.original_status}`);
      }

      realtimeResponse = await chatwootRequest<any>(this.getClientCwConfig(), {
        method: 'PATCH',
        url:
          `/api/v1/accounts/${this.provider.accountId}/conversations/${reference.chatwootConversationId}` +
          `/messages/${reference.chatwootMessageId}`,
        body: {
          status: originalStatusName,
          ...(originalStatusName === 'failed' && updatedRow.external_error
            ? { external_error: updatedRow.external_error }
            : {}),
        },
        mediaType: 'application/json',
      });
      realtimeNotified = true;
    } catch (error) {
      await this.pgClient.query(
        `UPDATE messages
         SET status = $1
         WHERE id = $2
           AND conversation_id = $3
           AND account_id = $4`,
        [originalStatus, reference.chatwootMessageId, reference.chatwootConversationId, this.provider.accountId],
      );
      this.logger.warn(
        `[CW.EDIT] Edited content persisted, but Chatwoot realtime notification failed chatwootMessageId=${
          reference.chatwootMessageId
        }; refresh the conversation to display it: ${this.formatError(error)}`,
      );
    }

    const verification = await this.pgClient.query(
      `SELECT id, content, processed_message_content, status
       FROM messages
       WHERE id = $1
         AND conversation_id = $2
         AND account_id = $3
       LIMIT 1`,
      [reference.chatwootMessageId, reference.chatwootConversationId, this.provider.accountId],
    );
    const verifiedRow = verification?.rows?.[0];

    if (
      !verifiedRow ||
      verifiedRow.content !== content ||
      verifiedRow.processed_message_content !== content ||
      Number(verifiedRow.status) !== originalStatus
    ) {
      throw new Error(`Chatwoot edited message verification failed for message ${reference.chatwootMessageId}`);
    }

    return {
      method: 'database',
      realtimeNotified,
      response: realtimeResponse || verifiedRow,
    };
  }

  private async getReplyToIds(
    msg: any,
    instance: InstanceDto,
  ): Promise<{ in_reply_to: string; in_reply_to_external_id: string }> {
    let inReplyTo = null;
    let inReplyToExternalId = null;

    if (msg) {
      inReplyToExternalId = this.getReplyContextInfo(msg)?.stanzaId || null;
      if (inReplyToExternalId) {
        const message = await this.getMessageByKeyId(instance, inReplyToExternalId);
        if (message?.chatwootMessageId) {
          inReplyTo = message.chatwootMessageId;
        }
      }
    }

    return {
      in_reply_to: inReplyTo,
      in_reply_to_external_id: inReplyToExternalId,
    };
  }

  private getReplyContextInfo(msg: any): any | null {
    const message = msg?.message || msg;
    const candidates = [
      msg?.contextInfo,
      message?.contextInfo,
      message?.extendedTextMessage?.contextInfo,
      message?.imageMessage?.contextInfo,
      message?.videoMessage?.contextInfo,
      message?.documentMessage?.contextInfo,
      message?.audioMessage?.contextInfo,
      message?.stickerMessage?.contextInfo,
      message?.buttonsResponseMessage?.contextInfo,
      message?.listResponseMessage?.contextInfo,
      message?.templateButtonReplyMessage?.contextInfo,
      message?.interactiveResponseMessage?.contextInfo,
      message?.documentWithCaptionMessage?.message?.documentMessage?.contextInfo,
      message?.ephemeralMessage?.message?.extendedTextMessage?.contextInfo,
      message?.viewOnceMessage?.message?.imageMessage?.contextInfo,
      message?.viewOnceMessage?.message?.videoMessage?.contextInfo,
      message?.viewOnceMessageV2?.message?.imageMessage?.contextInfo,
      message?.viewOnceMessageV2?.message?.videoMessage?.contextInfo,
    ];

    return candidates.find((contextInfo) => contextInfo?.stanzaId || contextInfo?.quotedMessage) || null;
  }

  private addMissingReplyFallback(content: string, msg: any, hasChatwootReply: boolean): string {
    if (hasChatwootReply) {
      return content;
    }

    const contextInfo = this.getReplyContextInfo(msg);
    if (!contextInfo?.stanzaId) {
      return content;
    }

    const quotedContent = this.getQuotedContentPreview(contextInfo.quotedMessage);
    const quotedLines = quotedContent.split('\n').map((line) => `> ${line}`);
    const fallback = ['↩️ **Respondendo a uma mensagem anterior:**', ...quotedLines].join('\n');

    return content ? `${fallback}\n\n${content}` : fallback;
  }

  private getQuotedContentPreview(quotedMessage: any): string {
    if (!quotedMessage) {
      return '💬 Mensagem indisponível no histórico do Chatwoot';
    }

    const documentMessage =
      quotedMessage.documentMessage || quotedMessage.documentWithCaptionMessage?.message?.documentMessage;
    const text = quotedMessage.conversation || quotedMessage.extendedTextMessage?.text;

    if (text) {
      return this.truncateReplyPreview(text);
    }

    if (documentMessage) {
      const documentLabel = `📎 Documento${documentMessage.fileName ? `: ${documentMessage.fileName}` : ''}`;
      return documentMessage.caption
        ? `${documentLabel} — ${this.truncateReplyPreview(documentMessage.caption)}`
        : documentLabel;
    }
    if (quotedMessage.imageMessage) {
      return quotedMessage.imageMessage.caption
        ? `🖼️ Imagem — ${this.truncateReplyPreview(quotedMessage.imageMessage.caption)}`
        : '🖼️ Imagem';
    }
    if (quotedMessage.videoMessage) {
      return quotedMessage.videoMessage.caption
        ? `🎥 Vídeo — ${this.truncateReplyPreview(quotedMessage.videoMessage.caption)}`
        : '🎥 Vídeo';
    }
    if (quotedMessage.audioMessage) return '🎧 Áudio';
    if (quotedMessage.stickerMessage) return '🏷️ Figurinha';
    if (quotedMessage.contactMessage || quotedMessage.contactsArrayMessage) return '👤 Contato';
    if (quotedMessage.locationMessage || quotedMessage.liveLocationMessage) return '📍 Localização';

    return '💬 Mensagem anterior';
  }

  private truncateReplyPreview(content: unknown, maxLength = 500): string {
    const normalizedContent = String(content).replace(/\s+/g, ' ').trim();
    return normalizedContent.length > maxLength
      ? `${normalizedContent.substring(0, maxLength - 3)}...`
      : normalizedContent;
  }

  private async getQuotedMessage(msg: any, instance: InstanceDto): Promise<Quoted> {
    if (msg?.content_attributes?.in_reply_to) {
      const message = await this.prismaRepository.message.findFirst({
        where: {
          chatwootMessageId: msg?.content_attributes?.in_reply_to,
          instanceId: instance.instanceId,
        },
      });

      const key = message?.key as WAMessageKey;
      const messageContent = message?.message as WAMessageContent;

      if (messageContent && key?.id) {
        return {
          key: key,
          message: messageContent,
        };
      }
    }

    return null;
  }

  private isMediaMessage(message: any) {
    const media = [
      'imageMessage',
      'documentMessage',
      'documentWithCaptionMessage',
      'audioMessage',
      'videoMessage',
      'stickerMessage',
      'viewOnceMessageV2',
    ];

    const messageKeys = Object.keys(message);

    const result = messageKeys.some((key) => media.includes(key));

    return result;
  }

  private isAudioMediaMessage(message: any) {
    return !!(
      message?.audioMessage ||
      message?.viewOnceMessageV2?.message?.audioMessage ||
      message?.ephemeralMessage?.message?.audioMessage
    );
  }

  private isInteractiveButtonMessage(messageType: string, message: any) {
    return messageType === 'interactiveMessage' && message.interactiveMessage?.nativeFlowMessage?.buttons?.length > 0;
  }

  private getAdsMessage(msg: any) {
    interface AdsMessage {
      title: string;
      body: string;
      thumbnailUrl: string;
      sourceUrl: string;
    }

    const adsMessage: AdsMessage | undefined = {
      title: msg.extendedTextMessage?.contextInfo?.externalAdReply?.title || msg.contextInfo?.externalAdReply?.title,
      body: msg.extendedTextMessage?.contextInfo?.externalAdReply?.body || msg.contextInfo?.externalAdReply?.body,
      thumbnailUrl:
        msg.extendedTextMessage?.contextInfo?.externalAdReply?.thumbnailUrl ||
        msg.contextInfo?.externalAdReply?.thumbnailUrl,
      sourceUrl:
        msg.extendedTextMessage?.contextInfo?.externalAdReply?.sourceUrl || msg.contextInfo?.externalAdReply?.sourceUrl,
    };

    return adsMessage;
  }

  private getReactionMessage(msg: any) {
    interface ReactionMessage {
      key: {
        id: string;
        fromMe: boolean;
        remoteJid: string;
        participant?: string;
      };
      text: string;
    }
    const reactionMessage: ReactionMessage | undefined = msg?.reactionMessage;

    return reactionMessage;
  }

  private getTypeMessage(msg: any) {
    const types = {
      conversation: msg.conversation,
      imageMessage: msg.imageMessage?.caption,
      videoMessage: msg.videoMessage?.caption,
      extendedTextMessage: msg.extendedTextMessage?.text,
      messageContextInfo: msg.messageContextInfo?.stanzaId,
      stickerMessage: undefined,
      documentMessage: msg.documentMessage?.caption,
      documentWithCaptionMessage: msg.documentWithCaptionMessage?.message?.documentMessage?.caption,
      audioMessage: msg.audioMessage ? (msg.audioMessage.caption ?? '') : undefined,
      contactMessage: msg.contactMessage?.vcard,
      contactsArrayMessage: msg.contactsArrayMessage,
      locationMessage: msg.locationMessage,
      liveLocationMessage: msg.liveLocationMessage,
      listMessage: msg.listMessage,
      listResponseMessage: msg.listResponseMessage,
      viewOnceMessageV2:
        msg?.message?.viewOnceMessageV2?.message?.imageMessage?.url ||
        msg?.message?.viewOnceMessageV2?.message?.videoMessage?.url ||
        msg?.message?.viewOnceMessageV2?.message?.audioMessage?.url,
    };

    return types;
  }

  private getMessageContent(types: any) {
    const typeKey = Object.keys(types).find((key) => types[key] !== undefined);

    let result = typeKey ? types[typeKey] : undefined;

    // Remove externalAdReplyBody| in Chatwoot (Already Have)
    if (result && typeof result === 'string' && result.includes('externalAdReplyBody|')) {
      result = result.split('externalAdReplyBody|').filter(Boolean).join('');
    }

    if (typeKey === 'locationMessage' || typeKey === 'liveLocationMessage') {
      const latitude = result.degreesLatitude;
      const longitude = result.degreesLongitude;

      const locationName = result?.name;
      const locationAddress = result?.address;

      const formattedLocation =
        `*${i18next.t('cw.locationMessage.location')}:*\n\n` +
        `_${i18next.t('cw.locationMessage.latitude')}:_ ${latitude} \n` +
        `_${i18next.t('cw.locationMessage.longitude')}:_ ${longitude} \n` +
        (locationName ? `_${i18next.t('cw.locationMessage.locationName')}:_ ${locationName}\n` : '') +
        (locationAddress ? `_${i18next.t('cw.locationMessage.locationAddress')}:_ ${locationAddress} \n` : '') +
        `_${i18next.t('cw.locationMessage.locationUrl')}:_ ` +
        `https://www.google.com/maps/search/?api=1&query=${latitude},${longitude}`;

      return formattedLocation;
    }

    if (typeKey === 'contactMessage') {
      const vCardData = result.split('\n');
      const contactInfo = {};

      vCardData.forEach((line) => {
        const [key, value] = line.split(':');
        if (key && value) {
          contactInfo[key] = value;
        }
      });

      let formattedContact =
        `*${i18next.t('cw.contactMessage.contact')}:*\n\n` +
        `_${i18next.t('cw.contactMessage.name')}:_ ${contactInfo['FN']}`;

      let numberCount = 1;
      Object.keys(contactInfo).forEach((key) => {
        if (key.startsWith('item') && key.includes('TEL')) {
          const phoneNumber = contactInfo[key];
          formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${phoneNumber}`;
          numberCount++;
        } else if (key.includes('TEL')) {
          const phoneNumber = contactInfo[key];
          formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${phoneNumber}`;
          numberCount++;
        }
      });

      return formattedContact;
    }

    if (typeKey === 'contactsArrayMessage') {
      const formattedContacts = result.contacts.map((contact) => {
        const vCardData = contact.vcard.split('\n');
        const contactInfo = {};

        vCardData.forEach((line) => {
          const [key, value] = line.split(':');
          if (key && value) {
            contactInfo[key] = value;
          }
        });

        let formattedContact = `*${i18next.t('cw.contactMessage.contact')}:*\n\n_${i18next.t(
          'cw.contactMessage.name',
        )}:_ ${contact.displayName}`;

        let numberCount = 1;
        Object.keys(contactInfo).forEach((key) => {
          if (key.startsWith('item') && key.includes('TEL')) {
            const phoneNumber = contactInfo[key];
            formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${phoneNumber}`;
            numberCount++;
          } else if (key.includes('TEL')) {
            const phoneNumber = contactInfo[key];
            formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${phoneNumber}`;
            numberCount++;
          }
        });

        return formattedContact;
      });

      const formattedContactsArray = formattedContacts.join('\n\n');

      return formattedContactsArray;
    }

    if (typeKey === 'listMessage') {
      const listTitle = result?.title || 'Unknown';
      const listDescription = result?.description || 'Unknown';
      const listFooter = result?.footerText || 'Unknown';

      let formattedList =
        '*List Menu:*\n\n' +
        '_Title_: ' +
        listTitle +
        '\n' +
        '_Description_: ' +
        listDescription +
        '\n' +
        '_Footer_: ' +
        listFooter;

      if (result.sections && result.sections.length > 0) {
        result.sections.forEach((section, sectionIndex) => {
          formattedList += '\n\n*Section ' + (sectionIndex + 1) + ':* ' + section.title || 'Unknown\n';

          if (section.rows && section.rows.length > 0) {
            section.rows.forEach((row, rowIndex) => {
              formattedList += '\n*Line ' + (rowIndex + 1) + ':*\n';
              formattedList += '_▪️ Title:_ ' + (row.title || 'Unknown') + '\n';
              formattedList += '_▪️ Description:_ ' + (row.description || 'Unknown') + '\n';
              formattedList += '_▪️ ID:_ ' + (row.rowId || 'Unknown') + '\n';
            });
          } else {
            formattedList += '\nNo lines found in this section.\n';
          }
        });
      } else {
        formattedList += '\nNo sections found.\n';
      }

      return formattedList;
    }

    if (typeKey === 'listResponseMessage') {
      const responseTitle = result?.title || 'Unknown';
      const responseDescription = result?.description || 'Unknown';
      const responseRowId = result?.singleSelectReply?.selectedRowId || 'Unknown';

      const formattedResponseList =
        '*List Response:*\n\n' +
        '_Title_: ' +
        responseTitle +
        '\n' +
        '_Description_: ' +
        responseDescription +
        '\n' +
        '_ID_: ' +
        responseRowId;
      return formattedResponseList;
    }

    return result;
  }

  public getConversationMessage(msg: any) {
    if (!msg) {
      return null;
    }

    const types = this.getTypeMessage(msg);

    const messageContent = this.getMessageContent(types);

    return messageContent;
  }

  public async eventWhatsapp(event: string, instance: InstanceDto, body: any) {
    try {
      const waInstance = this.waMonitor.waInstances[instance.instanceName];

      if (!waInstance) {
        this.logger.warn('wa instance not found');
        return null;
      }

      const client = await this.clientCw(instance);

      if (!client) {
        this.logger.warn('client not found');
        return null;
      }

      if (event === Events.CONTACTS_UPSERT || event === Events.CONTACTS_UPDATE) {
        const phoneJid = await this.resolveContactPhoneJid(
          instance,
          {
            remoteJid: body.remoteJid,
            remoteJidAlt: body.remoteJidAlt,
            remoteJidLid: body.remoteJid?.includes('@lid') ? body.remoteJid : undefined,
          },
          !!body.remoteJid?.includes('@g.us'),
        );
        const contactLookup = phoneJid?.includes('@g.us') ? phoneJid : phoneJid?.split('@')[0];

        if (!contactLookup) {
          return null;
        }

        const contact = phoneJid.includes('@g.us')
          ? await this.findContact(instance, contactLookup)
          : await this.reconcileChatwootContactIdentity(
              instance,
              phoneJid,
              body.remoteJid?.includes('@lid') ? body.remoteJid : null,
              body.pushName,
            );
        if (!contact) {
          return null;
        }

        const data: Record<string, string> = {};
        if (this.isUsableContactName(body.pushName) && !this.isUsableContactName(contact.name)) {
          data.name = body.pushName.trim();
        }
        if (body.profilePicUrl && body.profilePicUrl !== contact.thumbnail) {
          data.avatar_url = body.profilePicUrl;
        }

        if (Object.keys(data).length > 0) {
          return await this.updateContact(instance, contact.id, data);
        }

        return contact;
      }

      if (this.provider?.ignoreJids && this.provider?.ignoreJids.length > 0) {
        const ignoreJids: any = this.provider?.ignoreJids;

        let ignoreGroups = false;
        let ignoreContacts = false;

        if (ignoreJids.includes('@g.us')) {
          ignoreGroups = true;
        }

        if (ignoreJids.includes('@s.whatsapp.net')) {
          ignoreContacts = true;
        }

        if (ignoreGroups && body?.key?.remoteJid.endsWith('@g.us')) {
          this.logger.warn('Ignoring message from group: ' + body?.key?.remoteJid);
          return;
        }

        if (ignoreContacts && body?.key?.remoteJid.endsWith('@s.whatsapp.net')) {
          this.logger.warn('Ignoring message from contact: ' + body?.key?.remoteJid);
          return;
        }

        if (ignoreJids.includes(body?.key?.remoteJid)) {
          this.logger.warn('Ignoring message from jid: ' + body?.key?.remoteJid);
          return;
        }
      }

      if (event === 'messages.upsert' || event === 'send.message') {
        const embeddedEditedMessage = this.getEmbeddedEditedMessage(body);

        if (embeddedEditedMessage) {
          this.logger.info(
            `[CW.EDIT] Rerouting embedded edited message whatsappId=${
              embeddedEditedMessage.key?.id || 'unknown'
            } event=${event}`,
          );
          return await this.eventWhatsapp('messages.edit', instance, {
            key: embeddedEditedMessage.key,
            editedMessage: {
              message: embeddedEditedMessage.message,
            },
          });
        }

        if (this.isEditedMessageEnvelope(body)) {
          this.logger.warn(
            `[CW.EDIT] Blocked unparsed edited message envelope whatsappId=${body?.key?.id || 'unknown'} event=${event}`,
          );
          return;
        }

        this.logger.info(`[${event}] New message received - Instance: ${JSON.stringify(body, null, 2)}`);
        if (body.key.remoteJid === 'status@broadcast') {
          return;
        }

        if (body.message?.ephemeralMessage?.message) {
          body.message = {
            ...body.message?.ephemeralMessage?.message,
          };
        }

        const originalMessage = await this.getConversationMessage(body.message);
        const bodyMessage = originalMessage
          ? originalMessage
              .replaceAll(/\*((?!\s)([^\n*]+?)(?<!\s))\*/g, '**$1**')
              .replaceAll(/_((?!\s)([^\n_]+?)(?<!\s))_/g, '*$1*')
              .replaceAll(/~((?!\s)([^\n~]+?)(?<!\s))~/g, '~~$1~~')
          : originalMessage;

        if (bodyMessage && bodyMessage.includes('/survey/responses/') && bodyMessage.includes('http')) {
          return;
        }

        const quotedId = this.getReplyContextInfo(body)?.stanzaId;

        let quotedMsg = null;

        if (quotedId)
          quotedMsg = await this.prismaRepository.message.findFirst({
            where: {
              instanceId: instance.instanceId,
              key: {
                path: ['id'],
                equals: quotedId,
              },
              chatwootMessageId: {
                not: null,
              },
            },
          });

        const isMedia = this.isMediaMessage(body.message);

        const adsMessage = this.getAdsMessage(body);

        const reactionMessage = this.getReactionMessage(body.message);
        const isInteractiveButtonMessage = this.isInteractiveButtonMessage(body.messageType, body.message);

        if (!bodyMessage && !isMedia && !reactionMessage && !isInteractiveButtonMessage) {
          this.logger.warn(
            `No Chatwoot message content found whatsappId=${body?.key?.id || 'unknown'} messageType=${
              body?.messageType || 'unknown'
            } fields=${Object.keys(body?.message || {}).join(',') || 'none'}`,
          );
          return;
        }

        const existingMessageReference = body?.key?.id
          ? await this.findExistingChatwootMessageReference(instance, body.key.id)
          : null;

        if (existingMessageReference) {
          this.logger.info(
            `[CW.DEDUP] Reusing existing Chatwoot message whatsappId=${body.key.id} chatwootMessageId=${existingMessageReference.chatwootMessageId} conversationId=${existingMessageReference.chatwootConversationId}`,
          );
          return {
            id: existingMessageReference.chatwootMessageId,
            conversation_id: existingMessageReference.chatwootConversationId,
            inbox_id: existingMessageReference.chatwootInboxId,
          };
        }

        const getConversation = await this.createConversation(instance, body);

        if (!getConversation) {
          this.logger.warn('conversation not found');
          return;
        }

        const messageType = body.key.fromMe ? 'outgoing' : 'incoming';

        if (isMedia) {
          const isAudio = this.isAudioMediaMessage(body.message);
          const downloadBase64 = await waInstance?.getBase64FromMediaMessage({
            message: {
              ...body,
            },
            convertToMp4: isAudio,
          });

          if (!downloadBase64?.base64) {
            this.logger.warn(`media not downloaded for message ${body.key.id}`);
            return;
          }

          let nameFile: string;
          const messageBody = body?.message[body?.messageType];
          const originalFilename =
            messageBody?.fileName ||
            messageBody?.filename ||
            messageBody?.message?.documentMessage?.fileName ||
            downloadBase64.fileName;
          if (originalFilename) {
            const parsedFile = path.parse(originalFilename);
            const extension = downloadBase64.mimetype === 'audio/mp4' ? '.mp4' : parsedFile.ext;
            if (parsedFile.name && extension) {
              nameFile = `${parsedFile.name}-${Math.floor(Math.random() * (99 - 10 + 1) + 10)}${extension}`;
            }
          }

          if (!nameFile) {
            nameFile = `${Math.random().toString(36).substring(7)}.${
              mimeTypes.extension(downloadBase64.mimetype) || ''
            }`;
          }

          const fileData = Buffer.from(downloadBase64.base64, 'base64');

          const fileStream = new Readable();
          fileStream._read = () => {};
          fileStream.push(fileData);
          fileStream.push(null);

          if (body.key.remoteJid.includes('@g.us')) {
            const participantName = body.pushName;
            const rawPhoneNumber =
              body.key.addressingMode === 'lid' && !body.key.fromMe && body.key.participantAlt
                ? body.key.participantAlt.split('@')[0].split(':')[0]
                : body.key.participant.split('@')[0].split(':')[0];
            const formattedPhoneNumber =
              parsePhoneNumberFromString(`+${rawPhoneNumber}`)?.formatInternational() || `+${rawPhoneNumber}`;

            let content: string;

            if (!body.key.fromMe) {
              content = bodyMessage
                ? `**${formattedPhoneNumber} - ${participantName}:**\n\n${bodyMessage}`
                : `**${formattedPhoneNumber} - ${participantName}:**`;
            } else {
              content = bodyMessage || '';
            }

            const send = await this.sendData(
              getConversation,
              fileStream,
              nameFile,
              messageType,
              content,
              instance,
              body,
              'WAID:' + body.key.id,
              quotedMsg,
            );

            if (!send) {
              this.logger.warn('message not sent');
              return;
            }

            return send;
          } else {
            const send = await this.sendData(
              getConversation,
              fileStream,
              nameFile,
              messageType,
              bodyMessage,
              instance,
              body,
              'WAID:' + body.key.id,
              quotedMsg,
            );

            if (!send) {
              this.logger.warn('message not sent');
              return;
            }

            return send;
          }
        }

        if (reactionMessage) {
          if (reactionMessage.text) {
            const send = await this.createMessage(
              instance,
              getConversation,
              reactionMessage.text,
              messageType,
              false,
              [],
              {
                message: { extendedTextMessage: { contextInfo: { stanzaId: reactionMessage.key.id } } },
              },
              'WAID:' + body.key.id,
              quotedMsg,
            );
            if (!send) {
              this.logger.warn('message not sent');
              return;
            }
          }

          return;
        }

        if (isInteractiveButtonMessage) {
          const buttons = body.message.interactiveMessage.nativeFlowMessage.buttons;
          this.logger.info('is Interactive Button Message: ' + JSON.stringify(buttons));

          for (const button of buttons) {
            const buttonParams = JSON.parse(button.buttonParamsJson);
            const paymentSettings = buttonParams.payment_settings;

            if (button.name === 'payment_info' && paymentSettings[0].type === 'pix_static_code') {
              const pixSettings = paymentSettings[0].pix_static_code;
              const pixKeyType = (() => {
                switch (pixSettings.key_type) {
                  case 'EVP':
                    return 'Chave Aleatória';
                  case 'EMAIL':
                    return 'E-mail';
                  case 'PHONE':
                    return 'Telefone';
                  default:
                    return pixSettings.key_type;
                }
              })();
              const pixKey = pixSettings.key_type === 'PHONE' ? pixSettings.key.replace('+55', '') : pixSettings.key;
              const content = `*${pixSettings.merchant_name}*\nChave PIX: ${pixKey} (${pixKeyType})`;

              const send = await this.createMessage(
                instance,
                getConversation,
                content,
                messageType,
                false,
                [],
                body,
                'WAID:' + body.key.id,
                quotedMsg,
              );
              if (!send) this.logger.warn('message not sent');
            } else {
              this.logger.warn('Interactive Button Message not mapped');
            }
          }
          return;
        }

        const isAdsMessage = (adsMessage && adsMessage.title) || adsMessage.body || adsMessage.thumbnailUrl;
        if (isAdsMessage) {
          const imgBuffer = await axios.get(adsMessage.thumbnailUrl, { responseType: 'arraybuffer' });

          const extension = mimeTypes.extension(imgBuffer.headers['content-type']);
          const mimeType = extension && mimeTypes.lookup(extension);

          if (!mimeType) {
            this.logger.warn('mimetype of Ads message not found');
            return;
          }

          const random = Math.random().toString(36).substring(7);
          const nameFile = `${random}.${mimeTypes.extension(mimeType)}`;
          const fileData = Buffer.from(imgBuffer.data, 'binary');

          const img = await Jimp.read(fileData);
          await img.cover({
            w: 320,
            h: 180,
          });
          const processedBuffer = await img.getBuffer(JimpMime.png);

          const fileStream = new Readable();
          fileStream._read = () => {}; // _read is required but you can noop it
          fileStream.push(processedBuffer);
          fileStream.push(null);

          const truncStr = (str: string, len: number) => {
            if (!str) return '';

            return str.length > len ? str.substring(0, len) + '...' : str;
          };

          const title = truncStr(adsMessage.title, 40);
          const description = truncStr(adsMessage?.body, 75);

          const send = await this.sendData(
            getConversation,
            fileStream,
            nameFile,
            messageType,
            `${bodyMessage}\n\n\n**${title}**\n${description}\n${adsMessage.sourceUrl}`,
            instance,
            body,
            'WAID:' + body.key.id,
          );

          if (!send) {
            this.logger.warn('message not sent');
            return;
          }

          return send;
        }

        if (body.key.remoteJid.includes('@g.us')) {
          const participantName = body.pushName;
          const rawPhoneNumber =
            body.key.addressingMode === 'lid' && !body.key.fromMe && body.key.participantAlt
              ? body.key.participantAlt.split('@')[0].split(':')[0]
              : body.key.participant.split('@')[0].split(':')[0];
          const formattedPhoneNumber =
            parsePhoneNumberFromString(`+${rawPhoneNumber}`)?.formatInternational() || `+${rawPhoneNumber}`;

          let content: string;

          if (!body.key.fromMe) {
            content = `**${formattedPhoneNumber} - ${participantName}:**\n\n${bodyMessage}`;
          } else {
            content = `${bodyMessage}`;
          }

          const send = await this.createMessage(
            instance,
            getConversation,
            content,
            messageType,
            false,
            [],
            body,
            'WAID:' + body.key.id,
            quotedMsg,
          );

          if (!send) {
            this.logger.warn('message not sent');
            return;
          }

          return send;
        } else {
          const send = await this.createMessage(
            instance,
            getConversation,
            bodyMessage,
            messageType,
            false,
            [],
            body,
            'WAID:' + body.key.id,
            quotedMsg,
          );

          if (!send) {
            this.logger.warn('message not sent');
            return;
          }

          return send;
        }
      }

      if (event === Events.MESSAGES_DELETE) {
        const chatwootDelete = this.configService.get<Chatwoot>('CHATWOOT').MESSAGE_DELETE;

        if (chatwootDelete === true) {
          if (!body?.key?.id) {
            this.logger.warn('message id not found');
            return;
          }

          const message = await this.getMessageByKeyId(instance, body.key.id);

          if (message?.chatwootMessageId && message?.chatwootConversationId) {
            await this.prismaRepository.message.deleteMany({
              where: {
                key: {
                  path: ['id'],
                  equals: body.key.id,
                },
                instanceId: instance.instanceId,
              },
            });

            return await client.messages.delete({
              accountId: this.provider.accountId,
              conversationId: message.chatwootConversationId,
              messageId: message.chatwootMessageId,
            });
          }
        }
      }

      if (event === 'messages.edit' || event === 'send.message.update') {
        const whatsappMessageId = body?.key?.id;
        if (!whatsappMessageId) {
          this.logger.warn('[CW.EDIT] Missing original WhatsApp message ID');
          return;
        }

        const editedMessagePayload = body?.editedMessage?.message || body?.editedMessage;
        if (!editedMessagePayload) {
          this.logger.warn(`[CW.EDIT] Missing edited payload for WhatsApp message ${whatsappMessageId}`);
          return;
        }

        const editedMessageContentRaw =
          this.getConversationMessage(editedMessagePayload) ?? (typeof body?.text === 'string' ? body.text : undefined);

        const editedMessageContent = typeof editedMessageContentRaw === 'string' ? editedMessageContentRaw.trim() : '';

        if (!editedMessageContent) {
          this.logger.warn(`[CW.EDIT] Empty edited content for WhatsApp message ${body?.key?.id || 'unknown'}`);
          return;
        }

        const message = await this.resolveChatwootMessageReference(instance, whatsappMessageId, body?.key);

        if (!message) {
          this.logger.warn(
            `[CW.EDIT] Original message not found in Evolution or Chatwoot: whatsappId=${whatsappMessageId} instanceId=${instance.instanceId}`,
          );
          return;
        }

        const key = message.key as WAMessageKey & {
          addressingMode?: string;
          participantAlt?: string;
        };

        if (message && message.chatwootConversationId && message.chatwootMessageId) {
          let chatwootContent = editedMessageContent;

          if (key?.remoteJid?.includes('@g.us') && !key.fromMe) {
            const participantJid =
              key.addressingMode === 'lid' ? key.participantAlt || key.participant : key.participant;
            const rawPhoneNumber = participantJid?.split('@')[0].split(':')[0];
            const formattedPhoneNumber = rawPhoneNumber
              ? parsePhoneNumberFromString(`+${rawPhoneNumber}`)?.formatInternational() || `+${rawPhoneNumber}`
              : '';
            const participantName = message.pushName || rawPhoneNumber || '';
            chatwootContent = `**${formattedPhoneNumber} - ${participantName}:**\n\n${editedMessageContent}`;
          }

          try {
            const updateResult = await this.updateChatwootMessageContent(
              client,
              {
                chatwootConversationId: message.chatwootConversationId,
                chatwootMessageId: message.chatwootMessageId,
                chatwootInboxId: message.chatwootInboxId || undefined,
                chatwootContactInboxSourceId: message.chatwootContactInboxSourceId || undefined,
              },
              chatwootContent,
            );
            this.logger.info(
              `[CW.EDIT] Updated and verified Chatwoot message whatsappId=${body.key.id} chatwootMessageId=${
                message.chatwootMessageId
              } method=${updateResult.method} realtime=${updateResult.realtimeNotified}`,
            );
            return updateResult.response;
          } catch (error) {
            const errorDetails = {
              message: error?.message,
              status: error?.status || error?.response?.status,
              statusText: error?.statusText,
              body: error?.body || error?.response?.data,
            };
            this.logger.error(
              `[CW.EDIT] Error updating Chatwoot message ${message.chatwootMessageId}: ${JSON.stringify(errorDetails)}`,
            );
            return;
          }
        }

        this.logger.warn(
          `[CW.EDIT] Original message has no Chatwoot association: whatsappId=${body?.key?.id} conversationId=${message?.chatwootConversationId} chatwootMessageId=${message?.chatwootMessageId}`,
        );
        return;
      }

      if (event === 'messages.read') {
        if (!body?.key?.id || !body?.key?.remoteJid) {
          this.logger.warn('message id not found');
          return;
        }

        const message = await this.getMessageByKeyId(instance, body.key.id);
        const conversationId = message?.chatwootConversationId;
        const contactInboxSourceId = message?.chatwootContactInboxSourceId;

        if (conversationId) {
          let sourceId = contactInboxSourceId;
          const inbox = (await this.getInbox(instance)) as inbox & {
            inbox_identifier?: string;
          };

          if (!sourceId && inbox) {
            const conversation = (await client.conversations.get({
              accountId: this.provider.accountId,
              conversationId: conversationId,
            })) as conversation_show & {
              last_non_activity_message: { conversation: { contact_inbox: contact_inboxes } };
            };
            sourceId = conversation.last_non_activity_message?.conversation?.contact_inbox?.source_id;
          }

          if (sourceId && inbox?.inbox_identifier) {
            const url =
              `/public/api/v1/inboxes/${inbox.inbox_identifier}/contacts/${sourceId}` +
              `/conversations/${conversationId}/update_last_seen`;
            await chatwootRequest(this.getClientCwConfig(), {
              method: 'POST',
              url: url,
            });
          }
        }
        return;
      }

      if (event === 'status.instance') {
        const data = body;
        const inbox = await this.getInbox(instance);

        if (!inbox) {
          this.logger.warn('inbox not found');
          return;
        }

        const msgStatus = i18next.t('cw.inbox.status', {
          inboxName: inbox.name,
          state: data.status,
        });

        await this.createBotMessage(instance, msgStatus, 'incoming');
      }

      if (event === 'connection.update' && body.status === 'open') {
        const waInstance = this.waMonitor.waInstances[instance.instanceName];
        if (!waInstance) return;

        const now = Date.now();
        const timeSinceLastNotification = now - (waInstance.lastConnectionNotification || 0);

        // Se a conexão foi estabelecida via QR code, notifica imediatamente.
        if (waInstance.qrCode && waInstance.qrCode.count > 0) {
          const msgConnection = i18next.t('cw.inbox.connected');
          await this.createBotMessage(instance, msgConnection, 'incoming');
          waInstance.qrCode.count = 0;
          waInstance.lastConnectionNotification = now;
          chatwootImport.clearAll(instance);
        }
        // Se não foi via QR code, verifica o throttling.
        else if (timeSinceLastNotification >= 30000) {
          const msgConnection = i18next.t('cw.inbox.connected');
          await this.createBotMessage(instance, msgConnection, 'incoming');
          waInstance.lastConnectionNotification = now;
        } else {
          this.logger.warn(
            `Connection notification skipped for ${instance.instanceName} - too frequent (${timeSinceLastNotification}ms since last)`,
          );
        }
      }

      if (event === 'qrcode.updated') {
        if (body.statusCode === 500) {
          const erroQRcode = `🚨 ${i18next.t('qrlimitreached')}`;
          return await this.createBotMessage(instance, erroQRcode, 'incoming');
        } else {
          const fileData = Buffer.from(body?.qrcode.base64.replace('data:image/png;base64,', ''), 'base64');

          const fileStream = new Readable();
          fileStream._read = () => {};
          fileStream.push(fileData);
          fileStream.push(null);

          await this.createBotQr(
            instance,
            i18next.t('qrgeneratedsuccesfully'),
            'incoming',
            fileStream,
            `${instance.instanceName}.png`,
          );

          let msgQrCode = `⚡️${i18next.t('qrgeneratedsuccesfully')}\n\n${i18next.t('scanqr')}`;

          if (body?.qrcode?.pairingCode) {
            msgQrCode =
              msgQrCode +
              `\n\n*Pairing Code:* ${body.qrcode.pairingCode.substring(0, 4)}-${body.qrcode.pairingCode.substring(
                4,
                8,
              )}`;
          }

          await this.createBotMessage(instance, msgQrCode, 'incoming');
        }
      }
    } catch (error) {
      this.logger.error(
        `Error processing WhatsApp event=${event} instance=${instance.instanceName}: ${this.formatError(error)}`,
      );
    }
  }

  public normalizeJidIdentifier(remoteJid: string) {
    if (!remoteJid) {
      return '';
    }
    if (remoteJid.includes('@lid')) {
      return remoteJid;
    }
    return remoteJid.replace(/:\d+/, '').split('@')[0];
  }

  public startImportHistoryMessages(instance: InstanceDto) {
    if (!this.isImportHistoryAvailable()) {
      return;
    }

    this.createBotMessage(instance, i18next.t('cw.import.startImport'), 'incoming');
    this.scheduleImportHistoryMessages(instance, this.IMPORT_HISTORY_START_DELAY_MS);
  }

  private scheduleImportHistoryMessages(instance: InstanceDto, delayMs = this.IMPORT_HISTORY_IDLE_DELAY_MS) {
    const timerKey = instance.instanceName;
    const currentTimer = this.importHistoryTimers.get(timerKey);

    if (currentTimer) {
      clearTimeout(currentTimer);
    }

    const timer = setTimeout(() => {
      this.importHistoryTimers.delete(timerKey);
      this.importHistoryMessages(instance).catch((error) => {
        this.logger.error(`Error on scheduled Chatwoot history import: ${error?.toString?.() || error}`);
      });
    }, delayMs);

    this.importHistoryTimers.set(timerKey, timer);
  }

  private clearImportHistoryTimer(instance: InstanceDto) {
    const timerKey = instance.instanceName;
    const currentTimer = this.importHistoryTimers.get(timerKey);

    if (currentTimer) {
      clearTimeout(currentTimer);
      this.importHistoryTimers.delete(timerKey);
    }
  }

  public isImportHistoryAvailable() {
    const uri = this.configService.get<Chatwoot>('CHATWOOT').IMPORT.DATABASE.CONNECTION.URI;

    return uri && uri !== 'postgres://user:password@hostname:port/dbname';
  }

  public addHistoryMessages(instance: InstanceDto, messagesRaw: MessageModel[]) {
    if (!this.isImportHistoryAvailable()) {
      return;
    }

    chatwootImport.addHistoryMessages(instance, messagesRaw);
    this.scheduleImportHistoryMessages(instance);
  }

  public addHistoryContacts(instance: InstanceDto, contactsRaw: ContactModel[]) {
    if (!this.isImportHistoryAvailable()) {
      return;
    }

    return chatwootImport.addHistoryContacts(instance, contactsRaw);
  }

  public async importHistoryMessages(instance: InstanceDto) {
    if (!this.isImportHistoryAvailable()) {
      return;
    }

    if (this.importHistoryRunning.has(instance.instanceName)) {
      return;
    }

    this.clearImportHistoryTimer(instance);
    this.importHistoryRunning.add(instance.instanceName);

    try {
      this.provider = this.provider || (await this.getProvider(instance));

      if (!this.provider) {
        this.logger.warn('provider not found');
        return null;
      }

      await this.createBotMessage(instance, i18next.t('cw.import.importingMessages'), 'incoming');

      const totalMessagesImported = await chatwootImport.importHistoryMessages(
        instance,
        this,
        await this.getInbox(instance),
        this.provider,
      );
      await this.updateContactAvatarInRecentConversations(instance);

      const msg = Number.isInteger(totalMessagesImported)
        ? i18next.t('cw.import.messagesImported', { totalMessagesImported })
        : i18next.t('cw.import.messagesException');

      await this.createBotMessage(instance, msg, 'incoming');

      return totalMessagesImported;
    } catch (error) {
      this.logger.error(`Error importing Chatwoot history messages: ${error?.toString?.() || error}`);
      await this.createBotMessage(instance, i18next.t('cw.import.messagesException'), 'incoming');
      return null;
    } finally {
      this.importHistoryRunning.delete(instance.instanceName);
    }
  }

  public async importData(instance: InstanceDto) {
    if (!this.isImportHistoryAvailable()) {
      throw new Error('Chatwoot import database connection is not configured');
    }

    const provider = await this.getProvider(instance);
    if (!provider?.enabled) {
      throw new Error('Chatwoot integration is not enabled for this instance');
    }

    const inbox = await this.getInbox(instance);
    if (!inbox) {
      throw new Error('Chatwoot inbox not found');
    }

    const instanceRecord = instance.instanceId
      ? { id: instance.instanceId }
      : await this.prismaRepository.instance.findFirst({
          where: {
            name: instance.instanceName,
          },
          select: {
            id: true,
          },
        });

    if (!instanceRecord?.id) {
      throw new Error('Instance not found');
    }

    const instanceForImport = {
      ...instance,
      instanceId: instanceRecord.id,
    };

    const daysLimitToImport = provider.daysLimitImportMessages ?? 7;
    const timestampLimitToImport = dayjs().subtract(daysLimitToImport, 'days').unix();

    const [contactsRaw, messagesRaw, chatsRaw] = await Promise.all([
      this.prismaRepository.contact.findMany({
        where: {
          instanceId: instanceForImport.instanceId,
        },
      }),
      this.prismaRepository.message.findMany({
        where: {
          instanceId: instanceForImport.instanceId,
          messageTimestamp: {
            gte: timestampLimitToImport,
          },
        },
        orderBy: {
          messageTimestamp: 'asc',
        },
      }),
      this.prismaRepository.chat.findMany({
        where: {
          instanceId: instanceForImport.instanceId,
        },
      }),
    ]);

    const { groupNamesByJid, totalGroupsSynced } = await this.syncGroupNamesForImport(
      instanceForImport,
      chatsRaw,
      contactsRaw,
      messagesRaw,
    );
    const contactsForImport = this.prepareContactsForChatwootImport(
      instanceForImport.instanceId,
      contactsRaw,
      chatsRaw,
      messagesRaw,
      groupNamesByJid,
    );

    if ((provider.importContacts || provider.importMessages) && contactsForImport.length > 0) {
      this.addHistoryContacts(instanceForImport, contactsForImport);
    }

    if (provider.importMessages && messagesRaw.length > 0) {
      this.addHistoryMessages(
        instanceForImport,
        messagesRaw.filter((msg: any) => !chatwootImport.isIgnoredRemoteJid(msg.key?.remoteJid)),
      );
    }

    let totalContactsImported = 0;
    let totalMessagesImported = 0;
    const providerData: ChatwootDto = {
      ...provider,
      ignoreJids: Array.isArray(provider.ignoreJids) ? provider.ignoreJids.map((event) => String(event)) : [],
    };
    const totalGroupContactsUpdated = await this.repairChatwootGroupContactNames(providerData, groupNamesByJid);

    if (provider.importMessages) {
      totalMessagesImported =
        (await chatwootImport.importHistoryMessages(instanceForImport, this, inbox, provider)) ?? totalMessagesImported;
      await this.updateContactAvatarInRecentConversations(instanceForImport);
    } else if (provider.importContacts) {
      totalContactsImported =
        (await chatwootImport.importHistoryContacts(instanceForImport, providerData)) ?? totalContactsImported;
    }

    const waInstance = this.waMonitor.waInstances[instance.instanceName];
    waInstance?.clearCacheChatwoot();

    return {
      contactsFound: contactsRaw.length,
      messagesFound: messagesRaw.length,
      groupsSynced: totalGroupsSynced,
      groupContactsUpdated: totalGroupContactsUpdated,
      totalContactsImported: provider.importContacts
        ? totalContactsImported || contactsRaw.length
        : totalContactsImported,
      totalMessagesImported,
    };
  }

  private async syncGroupNamesForImport(
    instance: InstanceDto,
    chatsRaw: { remoteJid: string; name?: string | null }[],
    contactsRaw: ContactModel[],
    messagesRaw: MessageModel[],
  ): Promise<{ groupNamesByJid: Map<string, string>; totalGroupsSynced: number }> {
    const groupNamesByJid = new Map<string, string>();
    const syncedGroups: { remoteJid: string; name: string }[] = [];
    const groupsNeedingSync = new Set<string>();

    chatsRaw
      .filter((chat) => chat.remoteJid?.includes('@g.us') && this.isUsableGroupName(chat.remoteJid, chat.name))
      .forEach((chat) => groupNamesByJid.set(chat.remoteJid, this.normalizeGroupSubject(chat.name)));

    chatsRaw
      .filter((chat) => chat.remoteJid?.includes('@g.us') && !groupNamesByJid.has(chat.remoteJid))
      .forEach((chat) => groupsNeedingSync.add(chat.remoteJid));

    contactsRaw
      .filter((contact) => contact.remoteJid?.includes('@g.us') && !groupNamesByJid.has(contact.remoteJid))
      .forEach((contact) => groupsNeedingSync.add(contact.remoteJid));

    messagesRaw
      .map((message: any) => message.key?.remoteJid)
      .filter((remoteJid: string) => remoteJid?.includes('@g.us') && !groupNamesByJid.has(remoteJid))
      .forEach((remoteJid: string) => groupsNeedingSync.add(remoteJid));

    if (groupsNeedingSync.size === 0) {
      return { groupNamesByJid, totalGroupsSynced: 0 };
    }

    const waInstance = this.waMonitor.waInstances[instance.instanceName];
    const client = waInstance?.client;
    if (!client || !instance.instanceId) {
      return { groupNamesByJid, totalGroupsSynced: 0 };
    }

    try {
      const groups = Object.values((await client.groupFetchAllParticipating?.()) || {}) as any[];
      groups.forEach((group) => {
        const groupJid = group?.id || group?.JID || group?.jid;
        const groupName = this.normalizeGroupSubject(group?.subject || group?.Name || group?.name);

        if (groupJid && groupsNeedingSync.has(groupJid) && this.isUsableGroupName(groupJid, groupName)) {
          groupNamesByJid.set(groupJid, groupName);
          syncedGroups.push({ remoteJid: groupJid, name: groupName });
        }
      });

      await this.persistSyncedGroupNames(instance.instanceId, syncedGroups);
    } catch (error) {
      this.logger.warn(`Unable to fetch all WhatsApp groups for Chatwoot import: ${error?.toString?.() || error}`);
    }

    return { groupNamesByJid, totalGroupsSynced: new Set(syncedGroups.map((group) => group.remoteJid)).size };
  }

  private async persistSyncedGroupNames(instanceId: string, groups: { remoteJid: string; name: string }[]) {
    const uniqueGroups = Array.from(new Map(groups.map((group) => [group.remoteJid, group])).values());
    if (uniqueGroups.length === 0) {
      return;
    }

    await Promise.all(
      uniqueGroups.map((group) =>
        Promise.all([
          this.prismaRepository.chat.upsert({
            where: {
              instanceId_remoteJid: {
                instanceId,
                remoteJid: group.remoteJid,
              },
            },
            create: {
              instanceId,
              remoteJid: group.remoteJid,
              name: group.name,
            },
            update: {
              name: group.name,
            },
          }),
          this.prismaRepository.contact.upsert({
            where: {
              remoteJid_instanceId: {
                remoteJid: group.remoteJid,
                instanceId,
              },
            },
            create: {
              instanceId,
              remoteJid: group.remoteJid,
              pushName: group.name,
            },
            update: {
              pushName: group.name,
            },
          }),
        ]),
      ),
    );
  }

  private async repairChatwootGroupContactNames(
    provider: ChatwootDto,
    groupNamesByJid: Map<string, string>,
  ): Promise<number> {
    const groups = Array.from(groupNamesByJid.entries())
      .map(([remoteJid, name]) => ({ remoteJid, name: this.normalizeGroupSubject(name) }))
      .filter((group) => this.isUsableGroupName(group.remoteJid, group.name));

    if (groups.length === 0) {
      return 0;
    }

    const pgClient = postgresClient.getChatwootConnection();
    let totalUpdated = 0;

    for (const group of groups) {
      const result = await pgClient.query(
        `UPDATE contacts
          SET name = $1, updated_at = NOW()
          WHERE account_id = $2
            AND identifier = $3
            AND COALESCE(name, '') <> $1`,
        [`${group.name} (GROUP)`, provider.accountId, group.remoteJid],
      );
      totalUpdated += result?.rowCount ?? 0;
    }

    return totalUpdated;
  }

  private normalizeGroupSubject(name?: string | null) {
    return name?.replace(/\s*\(GROUP\)$/i, '').trim() || '';
  }

  private isUsableGroupName(remoteJid: string, name?: string | null) {
    const cleanName = this.normalizeGroupSubject(name);

    return !!cleanName && cleanName.toUpperCase() !== 'GROUP' && cleanName !== remoteJid.split('@')[0];
  }

  private async getStoredGroupName(instanceId: string, remoteJid: string): Promise<string | null> {
    const chat = await this.prismaRepository.chat.findFirst({
      where: { instanceId, remoteJid },
      select: { name: true },
    });

    return this.isUsableGroupName(remoteJid, chat?.name) ? this.normalizeGroupSubject(chat?.name) : null;
  }

  private prepareContactsForChatwootImport(
    instanceId: string,
    contactsRaw: ContactModel[],
    chatsRaw: { remoteJid: string; name?: string | null; profilePicUrl?: string | null }[],
    messagesRaw: MessageModel[],
    groupNamesByJid: Map<string, string>,
  ): ContactModel[] {
    const contactsByJid = new Map<string, ContactModel>();

    contactsRaw.forEach((contact) => contactsByJid.set(contact.remoteJid, contact));

    const ensureGroupContact = (remoteJid?: string) => {
      if (!remoteJid?.includes('@g.us')) {
        return;
      }

      const existingContact = contactsByJid.get(remoteJid);
      const groupName = groupNamesByJid.get(remoteJid);
      const chat = chatsRaw.find((item) => item.remoteJid === remoteJid);
      const pushName =
        groupName ||
        (this.isUsableGroupName(remoteJid, chat?.name) ? this.normalizeGroupSubject(chat?.name) : null) ||
        remoteJid.split('@')[0];

      contactsByJid.set(remoteJid, {
        ...(existingContact || {
          id: remoteJid,
          remoteJid,
          profilePicUrl: null,
          createdAt: new Date(),
          updatedAt: new Date(),
          instanceId,
        }),
        pushName,
      } as ContactModel);
    };

    chatsRaw.forEach((chat) => ensureGroupContact(chat.remoteJid));
    messagesRaw.forEach((message: any) => ensureGroupContact(message.key?.remoteJid));

    return Array.from(contactsByJid.values());
  }

  public async updateContactAvatarInRecentConversations(instance: InstanceDto, limitContacts = 100) {
    try {
      if (!this.isImportHistoryAvailable()) {
        return;
      }

      const client = await this.clientCw(instance);
      if (!client) {
        this.logger.warn('client not found');
        return null;
      }

      const inbox = await this.getInbox(instance);
      if (!inbox) {
        this.logger.warn('inbox not found');
        return null;
      }

      const recentContacts = await chatwootImport.getContactsOrderByRecentConversations(
        inbox,
        this.provider,
        limitContacts,
      );

      const contactIdentifiers = recentContacts
        .map((contact) => contact.identifier)
        .filter((identifier) => identifier !== null);

      const contactsWithProfilePicture = (
        await this.prismaRepository.contact.findMany({
          where: {
            instanceId: instance.instanceId,
            id: {
              in: contactIdentifiers,
            },
            profilePicUrl: {
              not: null,
            },
          },
        })
      ).reduce((acc: Map<string, ContactModel>, contact: ContactModel) => acc.set(contact.id, contact), new Map());

      recentContacts.forEach(async (contact) => {
        if (contactsWithProfilePicture.has(contact.identifier)) {
          client.contacts.update({
            accountId: this.provider.accountId,
            id: contact.id,
            data: {
              avatar_url: contactsWithProfilePicture.get(contact.identifier).profilePictureUrl || null,
            },
          });
        }
      });
    } catch (error) {
      this.logger.error(`Error on update avatar in recent conversations: ${error.toString()}`);
    }
  }

  public async syncLostMessages(
    instance: InstanceDto,
    chatwootConfig: ChatwootDto,
    prepareMessage: (message: any) => any,
  ) {
    try {
      if (!this.isImportHistoryAvailable()) {
        return;
      }
      if (!this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
        return;
      }

      const inbox = await this.getInbox(instance);

      const sqlMessages = `select * from messages m
      where account_id = ${chatwootConfig.accountId}
      and inbox_id = ${inbox.id}
      and created_at >= now() - interval '6h'
      order by created_at desc`;

      const messagesData = (await this.pgClient.query(sqlMessages))?.rows;
      const ids: string[] = messagesData
        .filter((message) => !!message.source_id)
        .map((message) => message.source_id.replace('WAID:', ''));

      const savedMessages = await this.prismaRepository.message.findMany({
        where: {
          Instance: { name: instance.instanceName },
          messageTimestamp: { gte: Number(dayjs().subtract(6, 'hours').unix()) },
          status: { not: 'EDITED' },
          AND: ids.map((id) => ({ key: { path: ['id'], not: id } })),
        },
      });

      const filteredMessages = savedMessages.filter(
        (msg: any) => !chatwootImport.isIgnoredRemoteJid(msg.key?.remoteJid),
      );
      const messagesRaw: any[] = [];
      for (const m of filteredMessages) {
        if (!m.message || !m.key || !m.messageTimestamp) {
          continue;
        }

        if (Long.isLong(m?.messageTimestamp)) {
          m.messageTimestamp = m.messageTimestamp?.toNumber();
        }

        messagesRaw.push(prepareMessage(m as any));
      }

      this.addHistoryMessages(
        instance,
        messagesRaw.filter((msg) => !chatwootImport.isIgnoredRemoteJid(msg.key?.remoteJid)),
      );

      await chatwootImport.importHistoryMessages(instance, this, inbox, this.provider);
      const waInstance = this.waMonitor.waInstances[instance.instanceName];
      waInstance.clearCacheChatwoot();
    } catch {
      return;
    }
  }
}
