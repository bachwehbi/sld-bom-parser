import { useState, useCallback } from 'react';
import { Message } from '../types';

export function useChat() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  const sendMessage = useCallback(async (content: string, activeFile?: string | null) => {
    if (isLoading) return;

    const userMsg: Message = { role: 'user', content };

    // Optimistically add user message + loading placeholder
    setMessages((prev) => [
      ...prev,
      userMsg,
      { role: 'assistant', content: '', isLoading: true },
    ]);
    setIsLoading(true);

    // Build history (exclude the placeholder)
    const history = [...messages, userMsg].map(({ role, content }) => ({ role, content }));

    try {
      const resp = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: history, active_file: activeFile ?? null }),
      });

      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

      const data = await resp.json();
      const assistantMsg: Message = {
        role: 'assistant',
        content: data.content,
        file_refs: data.file_refs ?? [],
      };

      // Replace loading placeholder with real response
      setMessages((prev) => [...prev.slice(0, -1), assistantMsg]);
    } catch (err) {
      const errorMsg: Message = {
        role: 'assistant',
        content: `Sorry, something went wrong: ${err instanceof Error ? err.message : 'Unknown error'}`,
      };
      setMessages((prev) => [...prev.slice(0, -1), errorMsg]);
    } finally {
      setIsLoading(false);
    }
  }, [isLoading, messages]);

  const clearChat = useCallback(() => {
    setMessages([]);
  }, []);

  return { messages, isLoading, sendMessage, clearChat };
}
