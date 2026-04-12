import { useState, useRef, useEffect, useCallback } from 'react';
import { Message, Diagram } from '../types';
import MessageBubble from './MessageBubble';

interface ChatPanelProps {
  messages: Message[];
  isLoading: boolean;
  diagrams: Diagram[];
  onSendMessage: (content: string, activeFile?: string | null) => void;
  onSelectDiagram: (d: Diagram) => void;
  selectedFileName: string | null;
}

const SUGGESTIONS = [
  'List all unprocessed diagrams',
  'How many components were extracted from each diagram?',
  'Which diagrams have a match rate below 90%?',
  'Extract the diagram I just uploaded',
];

export default function ChatPanel({
  messages, isLoading, diagrams,
  onSendMessage, onSelectDiagram, selectedFileName,
}: ChatPanelProps) {
  const [input, setInput]     = useState('');
  const bottomRef             = useRef<HTMLDivElement>(null);
  const textareaRef           = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSend = useCallback(() => {
    const text = input.trim();
    if (!text || isLoading) return;
    setInput('');
    onSendMessage(text, selectedFileName);
  }, [input, isLoading, onSendMessage, selectedFileName]);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const isEmpty = messages.length === 0;

  return (
    <div className="flex flex-col h-full">
      {/* Messages */}
      <div className="flex-1 overflow-y-auto min-h-0 px-4 py-6">
        {isEmpty ? (
          <div className="h-full flex flex-col items-center justify-center gap-6 animate-fade-in">
            <div className="text-center">
              <div className="w-12 h-12 rounded-xl bg-green-500/10 border border-green-500/20 flex items-center justify-center mx-auto mb-3">
                <svg className="w-6 h-6 text-green-500" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M13 3L4 14h7l-2 7 9-11h-7l2-7z" />
                </svg>
              </div>
              <h2 className="text-base font-semibold text-gray-300 mb-1">SLD BOM Agent</h2>
              <p className="text-sm text-gray-600 max-w-xs">
                Ask about extracted diagrams, trigger new extractions, or query component details.
              </p>
            </div>
            {/* Suggestion chips */}
            <div className="flex flex-wrap gap-2 justify-center max-w-sm">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => { setInput(s); textareaRef.current?.focus(); }}
                  className="px-3 py-1.5 text-xs text-gray-400 rounded-full border border-gray-800 hover:border-gray-600 hover:text-gray-200 hover:bg-gray-800/50 transition-all"
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-4 max-w-3xl mx-auto w-full">
            {messages.map((msg, i) => (
              <MessageBubble
                key={i}
                message={msg}
                diagrams={diagrams}
                onSelectDiagram={onSelectDiagram}
              />
            ))}
            <div ref={bottomRef} />
          </div>
        )}
      </div>

      {/* Input area */}
      <div className="shrink-0 px-4 py-3 border-t border-gray-800/50">
        <div className="max-w-3xl mx-auto">
          {/* Context chip: currently viewing */}
          {selectedFileName && (
            <div className="flex items-center gap-2 mb-2">
              <span className="text-[10px] text-gray-600">viewing:</span>
              <span className="inline-flex items-center gap-1 text-[10px] px-2 py-0.5 rounded-full bg-gray-800 border border-gray-700/50 text-gray-400">
                <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
                {selectedFileName}
              </span>
            </div>
          )}

          <div className={`flex items-end gap-3 px-4 py-3 bg-gray-900/60 border rounded-2xl transition-all duration-150 ${isLoading ? 'border-gray-700/30' : 'border-gray-700/50 hover:border-gray-600/60 focus-within:border-gray-600'}`}>
            <textarea
              ref={textareaRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask about diagrams, trigger extractions…"
              disabled={isLoading}
              rows={1}
              className="flex-1 bg-transparent text-sm text-gray-200 placeholder-gray-600 resize-none focus:outline-none min-h-[24px] max-h-[200px] disabled:opacity-50"
              style={{ overflowY: input.split('\n').length > 5 ? 'auto' : 'hidden' }}
            />
            <button
              onClick={handleSend}
              disabled={!input.trim() || isLoading}
              className="shrink-0 w-8 h-8 flex items-center justify-center rounded-xl bg-green-600 hover:bg-green-500 text-white transition-all disabled:opacity-30 disabled:cursor-not-allowed"
            >
              {isLoading ? (
                <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
              ) : (
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
                </svg>
              )}
            </button>
          </div>
          <p className="text-[10px] text-gray-700 text-center mt-2">Enter to send · Shift+Enter for new line</p>
        </div>
      </div>
    </div>
  );
}
