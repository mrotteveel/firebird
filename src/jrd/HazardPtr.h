/*
 *	PROGRAM:	Engine Code
 *	MODULE:		HazardPtr.h
 *	DESCRIPTION:	Use of hazard pointers in metadata cache
 *
 *  The contents of this file are subject to the Initial
 *  Developer's Public License Version 1.0 (the "License");
 *  you may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *  http://www.ibphoenix.com/main.nfs?a=ibphoenix&page=ibp_idpl.
 *
 *  Software distributed under the License is distributed AS IS,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied.
 *  See the License for the specific language governing rights
 *  and limitations under the License.
 *
 *  The Original Code was created by Alexander Peshkov
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2021 Alexander Peshkov <peshkoff@mail.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 *
 *
 */

#ifndef JRD_HAZARDPTR_H
#define JRD_HAZARDPTR_H

#define HZ_DEB(A)

#include "../common/classes/alloc.h"
#include "../common/classes/array.h"
#include "../common/gdsassert.h"
#include "fb_blk.h"

#include <cds/gc/dhp.h>
#include <cds/algo/atomic.h>

#include <type_traits>

namespace Jrd {

	class HazardObject
	{
	protected:
		void retire()
		{
			fb_assert(this);

			struct Disposer
			{
				void operator()(HazardObject* ho)
				{
					fb_assert(ho);
					delete ho;
				}
			};

			cds::gc::DHP::retire<Disposer>(this);
		}

		virtual ~HazardObject();
	};

	template <typename T>
	class HazardPtr : private cds::gc::DHP::Guard
	{
		typedef cds::gc::DHP::Guard inherited;
		static_assert(std::is_base_of<HazardObject, T>::value, "class derived from HazardObject should be used");

	public:
		HazardPtr() = default;

		HazardPtr(const atomics::atomic<T*>& from)
		{
			protect(from);
		}

		HazardPtr(const HazardPtr& copyFrom)
		{
			copy(copyFrom);
		}

		HazardPtr(HazardPtr&& moveFrom) = default;

		template <class T2>
		HazardPtr(const HazardPtr<T2>& copyFrom)
		{
			checkAssign<T2>();
			copy(copyFrom);
		}

		template <class T2>
		HazardPtr(HazardPtr<T2>&& moveFrom)
			: inherited(std::move(moveFrom))
		{
			checkAssign<T2>();
		}

		~HazardPtr()
		{ }

		T* getPointer() const
		{
			return get<T>();
		}

		T* releasePointer()
		{
			T* rc = get<T>();
			clear();
			return rc;
		}

		void set(const atomics::atomic<T*>& from)
		{
			protect(from);
		}

		/*/ atomically replaces 'where' with 'newVal', using *this as old value for comparison
		// always sets *this to actual data from 'where'
		bool replace(atomics::atomic<T*>& where, T* newVal)
		{
			T* val = get<T>();
			bool rc = where.compare_exchange_strong(val, newVal,
				std::memory_order_release, std::memory_order_acquire);
			assign(rc ? newVal : val);
			return rc;
		}
*/
		// atomically replaces 'where' with 'newVal', using *this as old value for comparison
		// sets *this to actual data from 'where' if replace failed
		bool replace2(atomics::atomic<T*>& where, T* newVal)
		{
			T* val = get<T>();
			bool rc = where.compare_exchange_strong(val, newVal,
				std::memory_order_release, std::memory_order_acquire);
			if(rc)
				assign(newVal);
			return rc;
		}

		void clear()
		{
			inherited::clear();
		}

		T* operator->()
		{
			return get<T>();
		}

		const T* operator->() const
		{
			return get<T>();
		}
/*
		template <typename R>
		R& operator->*(R T::*mem)
		{
			return (this->hazardPointer)->*mem;
		}
 */
		bool operator!() const
		{
			return !hasData();
		}

		bool hasData() const
		{
			return get_native() != nullptr;
		}

		bool operator==(const T* v) const
		{
			return get<T>() == v;
		}

		bool operator!=(const T* v) const
		{
			return get<T>() != v;
		}

		operator bool() const
		{
			return hasData();
		}

		HazardPtr& operator=(const HazardPtr& copyAssign)
		{
			copy(copyAssign);
			return *this;
		}

		HazardPtr& operator=(HazardPtr&& moveAssign)
		{
			inherited::operator=(std::move(moveAssign));
			return *this;
		}

		template <class T2>
		HazardPtr& operator=(const HazardPtr<T2>& copyAssign)
		{
			checkAssign<T2>();
			copy(copyAssign);
			return *this;
		}

		template <class T2>
		HazardPtr& operator=(HazardPtr<T2>&& moveAssign)
		{
			checkAssign<T2>();
			inherited::operator=(std::move(moveAssign));
			return *this;
		}

		void safePointer(T* ptr)
		{
			assign(ptr);
		}

	private:
		template <class T2>
		struct checkAssign
		{
			static_assert(std::is_trivially_assignable<T*&, T2*>::value, "Invalid type of pointer assigned");
		};
	};

	template <typename T>
	bool operator==(const T* v1, const HazardPtr<T> v2)
	{
		return v2 == v1;
	}

	template <typename T, typename T2>
	bool operator==(const T* v1, const HazardPtr<T2> v2)
	{
		return v1 == v2.getPointer();
	}

	template <typename T>
	bool operator!=(const T* v1, const HazardPtr<T> v2)
	{
		return v2 != v1;
	}


	// Shared read here means that any thread can read from vector using HP.
	// It can be modified only in single thread, and it's caller's responsibility
	// that modifying thread is single.

	template <typename T, FB_SIZE_T CAP>
	class SharedReadVector : public Firebird::PermanentStorage
	{
	public:
		class Generation : public HazardObject, public pool_alloc_rpt<T>
		{
		private:
			Generation(FB_SIZE_T size)
				: count(0), capacity(size)
			{ }

			FB_SIZE_T count, capacity;
			T data[1];

		public:
			static Generation* create(MemoryPool& p, FB_SIZE_T cap)
			{
				return FB_NEW_RPT(p, cap) Generation(cap);
			}

			FB_SIZE_T getCount() const
			{
				return count;
			}

			FB_SIZE_T getCapacity() const
			{
				return capacity;
			}

			const T& value(FB_SIZE_T i) const
			{
				fb_assert(i < count);
				return data[i];
			}

			T& value(FB_SIZE_T i)
			{
				fb_assert(i < count);
				return data[i];
			}

			bool hasSpace(FB_SIZE_T needs = 1) const
			{
				return count + needs <= capacity;
			}

			bool add(const Generation* from)
			{
				if (!hasSpace(from->count))
					return false;
				memcpy(&data[count], from->data, from->count * sizeof(T));
				count += from->count;
				return true;
			}

			T* add()
			{
				if (!hasSpace())
					return nullptr;
				return &data[count++];
			}

			void truncate(const T& notValue)
			{
				while (count && data[count - 1] == notValue)
					count--;
			}

			static void destroy(Generation* gen)
			{
				// delay delete - someone else may access it
				gen->retire();
			}
		};

		SharedReadVector(MemoryPool& p)
			: Firebird::PermanentStorage(p),
			  currentData(Generation::create(getPool(), CAP))
		{ }

		Generation* writeAccessor()
		{
			return currentData.load(std::memory_order_acquire);
		}

		HazardPtr<Generation> readAccessor() const
		{
			return HazardPtr<Generation>(currentData);
		}

		void grow(FB_SIZE_T newSize = 0)
		{
			for(;;)
			{
				HazardPtr<Generation> current(currentData);
				if (newSize && (current->getCapacity() >= newSize))
					return;

				FB_SIZE_T doubleSize = current->getCapacity() * 2;
				if (newSize > doubleSize)
					doubleSize = newSize;

				Generation* newGeneration = Generation::create(getPool(), doubleSize);
				Generation* oldGeneration = current.getPointer();
				newGeneration->add(oldGeneration);
				if (current.replace2(currentData, newGeneration))
				{
					Generation::destroy(oldGeneration);
					break;
				}
				else
				{
					// Use plain delete - this instance is not known to anybody else
					delete newGeneration;
				}
			}
		}

		~SharedReadVector()
		{
			Generation::destroy(currentData.load(std::memory_order_acquire));
		}

	private:
		atomics::atomic<Generation*> currentData;
	};

	class thread_db;
	class CacheObject
	{
	public:
		typedef unsigned Flag;
		virtual bool checkObject(thread_db* tdbb, Firebird::Arg::StatusVector&) /*const*/;
		virtual void afterUnlock(thread_db* tdbb, unsigned flags);
		virtual void lockedExcl [[noreturn]] (thread_db* tdbb) /*const*/;
		virtual const char* c_name() const = 0;
	};

} // namespace Jrd

#endif // JRD_HAZARDPTR_H
